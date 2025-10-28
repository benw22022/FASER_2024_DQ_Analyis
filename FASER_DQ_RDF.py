"""
FASER Data Quality File Maker
_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-

A script which produces the Data Quality files for the 
FASER electronic muon neutrino analysis

Slimmed down version of this code
https://gitlab.cern.ch/tboeckh/FASERRDFAnalysis/-/tree/electronic-neutrino-analysis-2023?ref_type=heads

Adapted to work with the new p0011/p0012 2024 data
"""

import os
import glob
import json
import shutil
import argparse
from pathlib import Path
import logging
from typing import List, Dict, Union

import ROOT
import yaml
import uproot
import numpy as np
from tqdm import tqdm



def get_run_number_lumi_dict(path_to_grls: str) -> Dict[int, float]:
    """
    Parse the .csv files in the Good Run List (GRL) directory to map the run number to the recorded luminosity
    args:
        `path_to_grls`: `str` - path to directory containing the .json GRL files
    
    returns:
        `run_lumi_dict`: `Dict` - Dictionary which maps run number to luminosity (in /fb) 
    """

    grl_csvs = glob.glob(f"{path_to_grls}/*.csv")

    if len(grl_csvs) == 0:
        print(f"Error: No GRLS .csv found on path {path_to_grls}!")
        raise OSError("No files found")    

    run_lumi_dict = {}

    for fpath in grl_csvs:
        with open(fpath, 'r') as f:
            for i, line in enumerate(f):
                if i == 0: continue
                if line.startswith('#'): continue

                spline = line.split(',')
                run_number = int(spline[0])
                lumi_rec = float(spline[3])

                run_lumi_dict[run_number] = lumi_rec / 1000 # pb^-1 -> fb^-1
    
    return run_lumi_dict


def make_excluded_times_cut(path_to_grls: str) -> str:
    """
    Function to parse the .json files in the Good Run List (GRL) directory to find excluded time periods in otherwise good runs
    Function parses these time periods to construct a cut string which can be applied as a filter in a ROOT.RDataFrame to 
    remove the bad time periods

    args:
        `path_to_grls`: `str` - path to directory containing the .json GRL files
    
    returns:
        `cut_str`: `str` - a string which can be used with ROOT.RDataFrame::Filter to filter out excluded time periods from runs

    raises:
        `OSError` if no `.json` file are found in `path_to_grls` directory
    """

    grl_jsons = glob.glob(f"{path_to_grls}/*.json")

    if len(grl_jsons) == 0:
        print(f"Error: No GRLS .json found on path {path_to_grls}!")
        raise OSError("No files found")

    excluded_times = {}

    n_excluded_times = 0
    for grl_file in grl_jsons:    
        with open(grl_file, 'r') as f:
            grl_dict = json.load(f)
            
            for run_number, run_info in grl_dict.items():

                if "excluded_list" not in run_info.keys(): continue

                excluded_times[run_number] = run_info['excluded_list']
                n_excluded_times += len(run_info['excluded_list'])
                # logging.info(f"Found {len(run_info['excluded_list'])} excluded periods for run {run_number}")

    if n_excluded_times == 0: return ""

    logging.info(f"Applying cuts to remove {n_excluded_times} excluded periods")

    cut_str = ""
    for run_number, exclusion_list in excluded_times.items():
        for i, exclusion_info in enumerate(exclusion_list):
            start_time = exclusion_info['start_utime']
            stop_time = exclusion_info['stop_utime']

            cut_str += f"((eventTime >= {start_time}) && (eventTime <= {stop_time}) && (run == {run_number}))"
            if n_excluded_times > 1:
                cut_str += " || "
            
    return cut_str.rstrip(" || ")


def make_good_times_cut(path_to_grls: str) -> str:
    """
    Function to parse the .json files in the Good Run List (GRL) directory to find the stable time periods in good runs
    Function parses these time periods to construct a cut string which can be applied as a filter in a ROOT.RDataFrame to 
    select for the stable periods.

    args:
        `path_to_grls`: `str` - path to directory containing the .json GRL files
    
    returns:
        `cut_str`: `str` - a string which can be used with ROOT.RDataFrame::Filter to filter out select for time periods from runs

    raises:
        `OSError` if no `.json` file are found in `path_to_grls` directory
    """

    grl_jsons = glob.glob(f"{path_to_grls}/*.json")

    if len(grl_jsons) == 0:
        print(f"Error: No GRLS .json found on path {path_to_grls}!")
        raise OSError("No files found")

    good_times = {}

    n_good_times = 0
    for grl_file in grl_jsons:   
        with open(grl_file, 'r') as f:
            grl_dict = json.load(f)
            
            for run_number, run_info in grl_dict.items():

                good_times[run_number] = run_info['stable_list']
                n_good_times += len(run_info['stable_list'])

    cut_str = ""
    for run_number, stable_list in good_times.items():
        
        for i, stable_info in enumerate(stable_list):
            start_time = stable_info['start_utime']
            stop_time = stable_info['stop_utime']

            cut_str += f"((eventTime >= {start_time}) && (eventTime <= {stop_time}) && (run == {run_number}))"

            if n_good_times > 1:
                cut_str += " || "
            
    return cut_str.rstrip(" || ")



def validate_file_list(file_list) -> List[str]:
    """
    File check while loops through input files and checks that they can be opened and that they contain the `nt` tree

    args: 
        `file_list`: `List[str]` - list of files to check
    
    returns:
        `good_files`: `List[str]` - list of files which are openable and contain the `nt` tree
    """

    bad_files = []
    for fpath in tqdm(file_list):
        try:
            data = uproot.open(fpath)
        except Exception as e:
            print(f"Error: Unable to open {fpath}")
            bad_files.append(fpath)
            continue
        
        key_found = False
        for key in data.keys():
            if 'nt' in key: 
                key_found = True
                break
        
        if not key_found:
            bad_files.append(fpath)
            print(f"Error: Unable to open {fpath}. Does not contain 'nt' tree. Available keys are {data.keys()}")
    
    good_files = [fpath for fpath in file_list if fpath not in bad_files]

    return good_files



def check_df_and_apply_alias(df: ROOT.RDataFrame, column_name: str, column_alias: str) -> ROOT.RDataFrame:
    """
    Function that checks whether `column_name` exists in an RDataFrame and if it does not, create `column` name 
    by aliasing it to `column_alias`

    args:
        `df`: `ROOT.RDataframe` - dataframe to apply aliases to
        `column_name`: `str` - the column name to create via alias if not present in df
        `column_alias`: `str` - the column that will be aliased to `column_name`

    returns:
        `df`: `ROOT.RDataFrame` - dataframe with the aliases applied (if required)
    """

    if column_name not in df.GetColumnNames():
        df = df.Alias(column_name, column_alias)
        logging.info(f"Aliasing {column_name} -> {column_alias}")

    return df


def alias_data(df: ROOT.RDataFrame, has_veto11) -> ROOT.RDataFrame:
    """
    The names of some variables changed with the introduction of prompt reco tag r0022 (in August 2025) 
    and some new ones were added.
    This function creates an alias which maps the old names onto the new ones
    This function is kinda horrible, but I couldn't think of a better way to do things

    args: 
        `df`: `ROOT.RDataframe` - dataframe to apply aliases to
    
    returns:
        `df`: `ROOT.RDataFrame` - dataframe with the aliases applied (if required)
    """

    veto_prefix_map = {}
    if not has_veto11:
        veto_prefix_map = {
        "Veto11_": "Veto10_", # fudge to get code to run on 2022/23 data
        }


    veto_variables = [
    "charge",
    "raw_peak",
    "raw_charge",
    "baseline",
    "baseline_rms",
    "status",
    "triggertime",
    "localtime",
    "bcidtime"
    ]

    caloLo_prefix_map = {
        "CaloLo0_": "Calo0_",
        "CaloLo1_": "Calo1_",
        "CaloLo2_": "Calo2_",
        "CaloLo3_": "Calo3_",
    }

    caloHi_prefix_map = {
        "CaloHi0_": "Calo0_",
        "CaloHi1_": "Calo1_",
        "CaloHi2_": "Calo2_",
        "CaloHi3_": "Calo3_",
    }

    calo_variables = [
    "nMIP",
    "E_dep",
    "E_EM",
    "peak",
    "width",
    "charge",
    "raw_peak",
    "raw_charge",
    "baseline",
    "baseline_rms",
    "status",
    "triggertime",
    "localtime",
    "bcidtime"]

    for old_prefix, new_prefix in veto_prefix_map.items():
        for varname in veto_variables:
            df = check_df_and_apply_alias(df, old_prefix+varname, new_prefix+varname)  

    for old_prefix, new_prefix in caloLo_prefix_map.items():
        for varname in calo_variables:
            df = check_df_and_apply_alias(df, old_prefix+varname, new_prefix+varname)  

    for old_prefix, new_prefix in caloHi_prefix_map.items():
        for varname in calo_variables:
            df = check_df_and_apply_alias(df, old_prefix+varname, new_prefix+varname)  

    #* Alias total calo variables
    df = check_df_and_apply_alias(df, "CaloHi_total_E_EM",  "Calo_total_E_EM")
    df = check_df_and_apply_alias(df, "CaloLo_total_E_EM",  "Calo_total_E_EM")
    df = check_df_and_apply_alias(df, "CaloHi_total_nMIP",  "Calo_total_nMIP")
    df = check_df_and_apply_alias(df, "CaloLo_total_nMIP",  "Calo_total_nMIP")
    df = check_df_and_apply_alias(df, "CaloHi_total_E_dep",  "Calo_total_E_dep")
    df = check_df_and_apply_alias(df, "CaloLo_total_E_dep",  "Calo_total_E_dep")
    df = check_df_and_apply_alias(df, "CaloHi_total_fit_E_EM",  "Calo_total_fit_E_EM")
    df = check_df_and_apply_alias(df, "CaloLo_total_fit_E_EM",  "Calo_total_fit_E_EM")
    df = check_df_and_apply_alias(df, "CaloHi_total_raw_E_EM",  "Calo_total_raw_E_EM")
    df = check_df_and_apply_alias(df, "CaloLo_total_raw_E_EM",  "Calo_total_raw_E_EM")

    return df


def book_per_run_hists(df: ROOT.RDataFrame, histogram_cfg: Dict, run_number: int=None) -> List:
    """
    Function to book histograms which need to be evaluated for each run seperately
    
    args:
        `df`: `ROOT.RDataFrame` - the dataframe used to fill the histograms
        `histogram_Dict`: `Dict` - A histogram config dict. The values of the config must have the following dict schema

        ```
        {
        name: str           # the column to histogram
        nbins: int          # number of bins
        min: float          # lowest bin edge
        max: float          # hight bin edge
        unit_scale: float  (optional) # factor by which to multiply column by to convert units. E.g. 1000 to convert MeV -> GeV
        cut: Dict (optional)             # A dict with an expression field and a name field
            {expression: str   # A cut to evaluate and apply to the data just for this histogram
             name: str         # A name to give this cut for the RDF cutflow
            }
        }
        ```

        `run_number`: `int`(optional) - the run number to select for if given. We usually just run 
        over one run per job so we don't need to set this. Option is there just in case your files
        contain multiple runs.

    returns:
       `per_run_histos`: `List` - List of histograms to be filled
    """

    if run_number:
        df_this_run = df.Filter(f"run == {run_number}", f"Run: {run_number}")
    else:
        df_this_run = df

    #TODO: Dunno if we want to make the weight configurable. I define a column in case we want to in the future
    df_this_run = df_this_run.Define("weight", f"1")

    per_run_histos = []

    available_columns = df.GetColumnNames()

    for conf in histogram_cfg.values():
        
        if conf['name'] not in available_columns:
                logging.error(f"Could not find {conf['name']} in dataframe. Check your histogram configs and definitions.")
                raise ValueError("Invalid histogram config")

        #* Set a new histogram for making this histogram
        df_this_hist = df_this_run
        
        #* Rescale column to get in new units if asked
        column_name = conf['name']
        unit_scale = conf.get('unit_scale', False)
        if unit_scale:
            new_column_name = f"{column_name}_times_{unit_scale}".replace('.', '_') # NB: cannot have '.' in column name
            df_this_hist = df_this_hist.Define(new_column_name, f"{column_name} * {unit_scale}")
            column_name = new_column_name

        #* Make a new dataframe with a cut applied if required, just for this histogram
        if conf.get('cut', False): 
            cut_expr = conf['cut'].get('expression', False)
            cut_name = conf['cut'].get('name', "")
            if not cut_expr:
                logging.warning(f"You tried to apply a cut for histogram {conf['name']} but no cut expression was given! No cut will be applied here.")
            else:
                df_this_hist = df_this_hist.Filter(cut_expr, cut_name)
        
        #* Now we book the histogram
        per_run_histos.append(df_this_hist.Histo1D((conf['name'], f"{conf['name']};{conf['name']};Events", conf['nbins'], conf['min'], conf['max']), column_name, "weight"))
    
    #* Now finally book the eventTime histogram. This is kinda awkward to define with a simple yaml due the upper/lower bin edges
    event_times = np.array(df_this_run.AsNumpy(["eventTime"])["eventTime"])
    per_run_histos.append(df_this_run.Histo1D(("eventTime", "eventTime;eventTime;Events", 100, np.amin(event_times)-1, np.amax(event_times)+1), f"eventTime", "weight"))

    return per_run_histos


def book_yield_hists(df: ROOT.RDataFrame, run_number: int) -> List:

    """
    Book the yield histograms for this run
    These are the total number of tracks and total calorimeter yield for this run
    args:
        `df`: `ROOT.RDataFrame` - The ROOT RDataFRame
        `run_number`: `int` -  The run number - required to get the bin edges right
    returns:
        `yield_hists`: `List` - List of histogram result pointers
    """

    yield_hists = []
    runs = [run_number]

    nruns = int(max(runs) - min(runs) + 1)
    rmin =  min(runs)
    rmax =  max(runs) + 1
    
    # Track yields
    yield_hists.append(df.Histo1D(("Yield", "Yield;Yield;Events", nruns, rmin, rmax), "run"))
    yield_hists.append(df.Histo1D(("TrkYield", "TrkYield;TrkYield;Events", nruns, rmin, rmax), "run", "NTracks"))
    yield_hists.append(df.Histo1D(("PosTrkYield", "PosTrkYield;PosTrkYield;Events", nruns, rmin, rmax), "run", "NPosTracks"))
    yield_hists.append(df.Histo1D(("NegTrkYield", "NegTrkYield;NegTrkYield;Events", nruns, rmin, rmax), "run", "NNegTracks"))
    
    yield_hists.append(df.Histo1D(("GoodTrkYield", "GoodTrkYield;GoodTrkYield;Events", nruns, rmin, rmax), "run", "NGoodTracks"))
    yield_hists.append(df.Histo1D(("GoodPosTrkYield", "GoodPosTrkYield;GoodPosTrkYield;Events", nruns, rmin, rmax), "run", "NGoodPosTracks"))
    yield_hists.append(df.Histo1D(("GoodNegTrkYield", "GoodNegTrkYield;GoodNegTrkYield;Events", nruns, rmin, rmax), "run", "NGoodNegTracks"))

    # Calorimeter yields
    yield_hists.append(df.Histo1D(("CaloHiYield", "CaloHiYield;CaloHiYield;Events", nruns, rmin, rmax), "run", "CaloHiYield"))
    yield_hists.append(df.Histo1D(("CaloLoYield", "CaloLoYield;CaloLoYield;Events", nruns, rmin, rmax), "run", "CaloLoYield"))


    return yield_hists


def build_dataframe(file_list: List[str], tree: str='nt') -> ROOT.RDataFrame:
    """
    Function which constructs a ROOT RDataFrame from file(s) in `file_list`
    Applys the neccessary column definitions, aliases and data quality cuts

    args:
        `file_list`: `List[str]` - list of files to load
        `tree`: `str` - the name of the tree in the NTuples to read (default='nt')

    returns:
        `df`: `ROOT.RDataFrame` - the filtered dataframe with columns defined
    """

    df = ROOT.RDataFrame(tree, file_list)
    # ROOT.RDF.Experimental.AddProgressBar(df)

    #* Check if run is from 2022/23 - we didn't have Veto Station 11 for these years
    has_veto11 = True
    if args.run < 1.2e4: 
        has_veto11 = False 

    #* Apply aliases to map old variable names to their new ones
    df = alias_data(df, has_veto11)

    #* Allow shorter use of vecops functions in strings
    # e.g. DeltaPhi rather than ROOT::VecOps::DeltaPhi 
    ROOT.gInterpreter.Declare("using namespace ROOT::VecOps;")

    #* C++ defines (must not rely on anything defined below)
    ROOT.gInterpreter.Declare('#include "RDFDefines.h"')

    #* Data quality cuts
    good_times_cut_str = make_good_times_cut(args.grl_path)           # Select for the periods of stable running
    df = df.Define("GoodTimes", good_times_cut_str)
    df = df.Filter("GoodTimes", "Good times")
    
    exlcuded_times_cut_str = make_excluded_times_cut(args.grl_path)   # Some runs have certain time periods excluded. These periods are recorded in the GRL json files.
    if exlcuded_times_cut_str != "":
        df = df.Define("ExcludedTimes", exlcuded_times_cut_str)
        df = df.Filter("!ExcludedTimes", "Excluded times")
    
    df = df.Filter("(Timing0_status & 4) == 0 && (Timing1_status & 4) == 0 && (Timing2_status & 4) == 0 && (Timing3_status & 4) == 0 ", "No timing saturation")
    df = df.Filter("distanceToCollidingBCID == 0", "Colliding")
    df = df.Filter("(TAP & 4) != 0", "Timing Trigger")

    #* Definitions
    df = df.Define("NTracks", "Track_nLayers.size()")
    df = df.Define("NPosTracks", "Track_nLayers[Track_charge > 0].size()")
    df = df.Define("NNegTracks", "Track_nLayers[Track_charge < 0].size()")
    df = df.Define("Track_nHits", "Track_nDoF + 5")        
    df = df.Define("Track_chi2_per_dof", "Track_Chi2/Track_nDoF")
    df = df.Define("GoodTracks", "Track_nLayers >= 7 && Track_chi2_per_dof < 25 && Track_nHits >= 12 && Track_pz0 > 20000 && RemoveDuplicates(Track_p0)" )
    df = df.Define("NGoodTracks", "Track_nLayers[GoodTracks].size()")
    df = df.Define("NGoodPosTracks", "Track_nLayers[GoodTracks && Track_charge > 0].size()")
    df = df.Define("NGoodNegTracks", "Track_nLayers[GoodTracks && Track_charge < 0].size()")    
    df = df.Define("Track_pz_charge0", "Track_pz0 * Track_charge")
    df = df.Define("Track_theta_x1", "asin(Track_px1/Track_p1)")
    df = df.Define("Track_theta_y1", "asin(Track_py1/Track_p1)")
    df = df.Define("Track_pt0", "sqrt(Track_px0*Track_px0 + Track_py0*Track_py0)")
    df = df.Define("Track_theta0", "asin(Track_pt0/Track_p0)")
    df = df.Define("Track_phi0", "acos(Track_px0/Track_pt0)")
    df = df.Define("Track_eta0", "-log(tan(Track_theta0/2))")
    df = df.Define("Track_theta_x0", "asin(Track_px0/Track_p0)")
    df = df.Define("Track_theta_y0", "asin(Track_py0/Track_p0)")
    df = df.Define("Track_theta_x0_pos", "Track_theta_x0[Track_charge > 0]")
    df = df.Define("Track_theta_x0_neg", "Track_theta_x0[Track_charge < 0]")
    df = df.Define("Track_theta_y0_pos", "Track_theta_y0[Track_charge > 0]")
    df = df.Define("Track_theta_y0_neg", "Track_theta_y0[Track_charge < 0]")
    df = df.Define("Track_x0_pos", "Track_x0[Track_charge > 0]")
    df = df.Define("Track_x0_neg", "Track_x0[Track_charge < 0]")
    df = df.Define("Track_y0_pos", "Track_y0[Track_charge > 0]")
    df = df.Define("Track_y0_neg", "Track_y0[Track_charge < 0]")

    df = df.Define("Timing_charge_bottom", "Timing0_raw_charge + Timing1_raw_charge")
    df = df.Define("Timing_charge_top", "Timing2_raw_charge + Timing3_raw_charge")
    df = df.Define("Timing_charge_total", "Timing_charge_top + Timing_charge_bottom")
    
    df = df.Define("hitsVetoNu0", "VetoNu0_raw_charge > 40")
    df = df.Define("hitsVetoNu1", "VetoNu1_raw_charge > 40")
    
    df = df.Define("hitsVeto10", "Veto10_raw_charge > 40")
    df = df.Define("hitsVeto11", "Veto11_raw_charge > 40")
    df = df.Define("hitsVeto20", "Veto20_raw_charge > 40")
    df = df.Define("hitsVeto21", "Veto21_raw_charge > 40")

    df = df.Define(f"hitsTiming", "((Track_Y_atTrig[0] > 20 && Timing_charge_top > 20) || \
                                           (Track_Y_atTrig[0] < -20 && Timing_charge_bottom > 20) || \
                                           (Track_Y_atTrig[0] > -20 && Track_Y_atTrig[0] < 20 && Timing_charge_total > 20))")
    
    df = df.Define("hitsTiming0", "Timing0_status == 0")
    df = df.Define("hitsTiming1", "Timing1_status == 0")
    df = df.Define("hitsTiming2", "Timing2_status == 0")
    df = df.Define("hitsTiming3", "Timing3_status == 0")
    
    df = df.Define("hitsPreshower0", "Preshower0_raw_charge > 2.5")
    df = df.Define("hitsPreshower1", "Preshower1_raw_charge > 2.5")

    df = df.Define("hitsCaloLo0", "CaloLo0_status == 0")
    df = df.Define("hitsCaloLo1", "CaloLo1_status == 0")
    df = df.Define("hitsCaloLo2", "CaloLo2_status == 0")
    df = df.Define("hitsCaloLo3", "CaloLo3_status == 0")

    # Brian says that the double peaks in the CaloHi channel are coming from muons hitting the PMTs rather than energy deposits
    # He suggests requiring that the CaloLo signal is at least 10x  higher than the CaloHi signal
    df = df.Define("hitsCaloHi0", "(CaloHi0_status == 0) && (CaloLo0_raw_charge > 10 * CaloHi0_raw_charge)")
    df = df.Define("hitsCaloHi1", "(CaloHi1_status == 0) && (CaloLo1_raw_charge > 10 * CaloHi1_raw_charge)")
    df = df.Define("hitsCaloHi2", "(CaloHi2_status == 0) && (CaloLo2_raw_charge > 10 * CaloHi2_raw_charge)")
    df = df.Define("hitsCaloHi3", "(CaloHi3_status == 0) && (CaloLo3_raw_charge > 10 * CaloHi3_raw_charge)")

    #Calorimeter Yields Variables
    GeV = 1000
    CaloHiCount = df.Filter(f"CaloHi_total_E_EM > {100*GeV}").Count()
    CaloLoCount = df.Filter(f"CaloLo_total_E_EM > {10*GeV} && CaloLo_total_E_EM < {100*GeV}").Count()

    CaloHiYield = CaloHiCount.GetValue()
    CaloLoYield = CaloLoCount.GetValue()

    df = df.Define("CaloHiYield", f"{CaloHiYield}").Define("CaloLoYield", f"{CaloLoYield}")
    return df


def parse_input_filelists(input_file_list_dir: str) -> Dict:
    """
    This function parses as directory containing a list of plain .txt files
    The format of the files must have one file path per line
    The run number is the name of the parent directory of the root file

    args:
        `input_file_list_dir`: `str` - Path to directory containing `.txt` files, listing filepaths to NTuple files
    
    returns:
        `file_dict`: `Dict` - A dictionary which maps the run number to a list of NTuple files for that run

    """

    txt_files = glob.glob(f"{input_file_list_dir}/*.txt")

    file_dict = {}

    for fpath in txt_files:
        with open(fpath, 'r') as f:
            for line in f:
                if line.startswith("#"): continue

                the_file_path = line.strip().strip("\n")
                the_file_name = os.path.basename(the_file_path)
                the_run_number = the_file_name.split("-")[2]
                try:
                    the_run_number = int(the_run_number)
                except ValueError as e:
                    print(f"")

                if the_run_number in file_dict.keys():
                    file_dict[the_run_number].append(the_file_path)
                else:
                    file_dict[the_run_number] = [the_file_path]    
    return file_dict


def parse_histogram_configs(histogram_cfg_dir: str) -> Dict:
    """
    Looks in a directory for `.yaml` files, checks if they have the key `histograms` then joins them together
    args:
       `histogram_cfg_dir`: str - path to directory containing histogram configs
    returns:
        `config_dict`: dictionary of histogram configs
    """

    cfg_fpaths = glob.glob(f"{histogram_cfg_dir}/*.yaml")

    logging.info(f"Found {len(cfg_fpaths)} histogram config files")

    config_dict = {}

    for fpath in cfg_fpaths:
        with open(fpath, 'r') as f:
            this_cfg_dict = yaml.safe_load(f)

            if not this_cfg_dict.get('histograms', False):
                logging.error(f"The config file {fpath} does not contain the key `histograms`. Please check the format of your config file.")
                raise ValueError("Invalid histogram config")

            #* Validate that each entry in the config has the requisite keys
            required_keys = {'name', 'nbins', 'min', 'max'}
            for key, cfg_dict in this_cfg_dict['histograms'].items():
                if not required_keys <= cfg_dict.keys():
                    logging.error(f"Histogram {key} in {fpath} is missing required keys. Missing keys: {required_keys - cfg_dict.keys()}. Check your config file.")
                    raise ValueError("Invalid Histogram config")
            
            config_dict = config_dict | this_cfg_dict['histograms']


    return config_dict


def main(args: argparse.Namespace) -> None:

    #* Enable multithreading
    ROOT.ROOT.EnableImplicitMT()

    #* Parse input files
    all_files_dict = parse_input_filelists(args.input_file_list_dir)
    file_list = all_files_dict[args.run]

    if len(file_list) <= 0:
        print("Error: Found no files to run over")
        return 1

    logging.info(f"Running over {len(file_list)} files for run {args.run}")
    for file in file_list:
        print(f"    - {file}")

    #* parse histogram configs
    histogram_config = parse_histogram_configs(args.histograms)

    #* Get lumi dict
    lumi_dict = {}
    lumi_dict = get_run_number_lumi_dict(args.grl_path)
    run_lumi = lumi_dict.get(args.run, None)
    logging.info(f"Run {args.run} luminosity = {run_lumi:.3f} /fb")

    #* Construct dataframe
    df = build_dataframe(file_list)
    yield_hists = book_yield_hists(df, args.run)
    run_hists = book_per_run_hists(df, histogram_config, args.run)

    #* Make output file (and output directory if needs be)
    output_file = f"{args.run}.root"
    file = ROOT.TFile(output_file, "RECREATE")
    tree = ROOT.TTree("dq", "Data Quality")
    logging.info(f"Writing output to {output_file}")
    
    #* Write out run number and lumi for convenience
    lumi_branch = ROOT.std.vector("float")()
    lumi_branch.push_back(run_lumi)
    tree.Branch("lumi", lumi_branch)

    run_num_branch = ROOT.std.vector("int")()
    run_num_branch.push_back(args.run)
    tree.Branch("run_number", run_num_branch)


    #* Write histograms
    for h in tqdm(yield_hists, desc="Info: Filling yield hists: "):
        h.Write()
    for h in tqdm(run_hists, desc="Info: Filling run hists: "):
        h.Write()

    #* Close the file
    tree.Fill()
    tree.Write()
    file.Close()
    logging.info(f"Wrote output to {output_file}")

    #* Move output file to output directory
    os.makedirs(args.output_file_dir, exist_ok=True)
    os.makedirs(f"{args.output_file_dir}/logs", exist_ok=True) # just in case the log directory doesn't exist
    logging.info(f"transferring output file: {output_file} -> {args.output_file_dir}/{output_file}")
    shutil.move(output_file, f"{args.output_file_dir}/{output_file}")
    

    #* Print cutflow
    logging.info("Cutflow Report:")
    cutReport = df.Report()
    cutReport.Print()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("run", type=int, help="Run to select")
    parser.add_argument("--input_file_list_dir", "-i", help="directory to txt files containing the available NTuple paths", default=f"{os.getcwd()}/faser_filelists")
    parser.add_argument("--output_file_dir", "-o", type=str, default="output", help = "Output file directory")
    parser.add_argument("--histograms", "-c", type=str, default=f"{os.getcwd()}/histograms", help = "Directory containing the histogram config files")
    parser.add_argument("-v", "--verbose",  help='If flag set then print debug messages', action='store_true')
    parser.add_argument("--grl_path", "-g", type=str, default="/cvmfs/faser.cern.ch/repo/sw/runlist/v8", help = "Path to directory containing GRL files in the .json format")
    args = parser.parse_args()

    # Print our arguments
    for key in vars(args):
        print(f"\t {key:<30}: {getattr(args, key)}")

    # Set log level
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Make sure all path args are absolute paths so condor doesn't get lost
    args.input_file_list_dir = os.path.abspath(args.input_file_list_dir)
    args.output_file_dir = os.path.abspath(args.output_file_dir)
    args.grl_path = os.path.abspath(args.grl_path)

    main(args)
