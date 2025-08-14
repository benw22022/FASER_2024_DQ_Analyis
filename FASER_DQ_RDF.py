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
import argparse
from pathlib import Path
from typing import List, Dict

import yaml
import ROOT
import uproot
import numpy as np
from tqdm import tqdm



def get_run_number_lumi_dict(path_to_grls: str) -> Dict[int, float]:
    """
    Parse the .csv files in the Good Run List (GRL) directory to map the run number to the recorded luminosity
    args:
        path_to_grls: str - path to directory containing the .json GRL files
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
        path_to_grls: str - path to directory containing the .json GRL files
    
    returns:
        cut_str: str - a string which can be used with ROOT.RDataFrame::Filter to filter out excluded time periods from runs

    raises:
        OSError if no .json file are found in `path_to_grls` directory
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
                # print(f"Info: Found {len(run_info['excluded_list'])} excluded periods for run {run_number}")

    if n_excluded_times == 0: return ""

    print(f"Info: Applying cuts to remove {n_excluded_times} excluded periods")

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
        path_to_grls: str - path to directory containing the .json GRL files
    
    returns:
        cut_str: str - a string which can be used with ROOT.RDataFrame::Filter to filter out select for time periods from runs


    raises:
        OSError if no .json file are found in `path_to_grls` directory
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


def parse_sample_yaml(fpath: str, keyname:str='samples') -> List[str]:
    """
    Function to parse a yaml file containing sample filepaths
    Automatically expands wildcards in paths

    args:
        fpath: str - file path to yaml file to load
        keyname: str (optional) - the name of the key in the yaml file which contains the list of file paths to return (default = 'samples')

    returns:
        files: List[str] - a list if the yaml files to load
    
    raises:
        OSError if `fpath` does not exist 
    """

    if not Path(fpath).exists():
        print(f"Error: Unable to find sample file {fpath}")
        raise OSError("Unable to open file")

    with open(fpath, 'r') as f:
        result_dict = yaml.safe_load(f)

        if keyname not in result_dict.keys():
            print(f"Warning: samples file {fpath} does not contain required key '{keyname}'")

        files = result_dict.get(keyname, [])
        files = [glob.glob(f) for f in files]   # Expand any wildcards
        files = [x for xs in files for x in xs] # Flatten nested list
        
        print(f"Info: Found {len(files)} in {fpath}")
        
        return files


def validate_file_list(file_list) -> List[str]:
    """
    File check while loops through input files and checks that they can be opened and that they contain the `nt` tree

    args: 
        file_list: List[str] - list of files to check
    
    returns:
        good_files: List[str] - list of files which are openable and contain the `nt` tree
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




def get_per_run_filelists(input_files: List[str]) -> List[str]:
    """
    Function which can generate a list of file from a list of `.yaml` files or a simple list of file paths (if latter this function doesn't really do anything)
    TODO: Support more input file type e.g. JSON, INI etc... ?

    args:
        input_files: List[str] - list of input files. If files end in `.yaml` then try and load them from the `.yaml` file
    
    returns:
        input_file_list: List[str] - a list of the file paths parsed

    raises:
        OSError: if input_files are not .root or .yaml
    """

    input_file_list = []
    for fpath in input_files:
        if fpath.endswith(".yaml"):
            files = parse_sample_yaml(fpath)
            input_files += files

        elif fpath.endswith(".root"):
            input_file_list.append(fpath)
        else:
            raise OSError(f"Error: Cannot read {fpath}. Files must be either `.root` or `.yaml`.")

    input_file_list = validate_file_list(input_file_list)

    return input_file_list


def check_df_and_apply_alias(df: ROOT.RDataFrame, column_name: str, column_alias: str) -> ROOT.RDataFrame:
    """
    Function that checks whether `column_name` exists in an RDataFrame and if it does not, create `column` name 
    by aliasing it to `column_alias`

    args:
        df: ROOT.RDataframe - dataframe to apply aliases to
        column_name: str - the column name to create via alias if not present in df
        column_alias: str - the column that will be aliased to `column_name`

    returns:
        df: ROOT.RDataFrame - dataframe with the aliases applied (if required)
    """

    if column_name not in df.GetColumnNames():
        df = df.Alias(column_name, column_alias)
        print(f"Info: Aliasing {column_name} -> {column_alias}")

    return df


def alias_p0012_data(df: ROOT.RDataFrame) -> ROOT.RDataFrame:
    """
    The names of some variables changed with the introduction of prompt reco tag p0012 (in August 2024) 
    This function creates an alias which maps the new names back onto the old ones

    args: 
        df: ROOT.RDataframe - dataframe to apply aliases to
    
    returns:
        df: ROOT.RDataFrame - dataframe with the aliases applied (if required)
    """
    

    veto_prefix_map = {
    "VetoSt10_": "Veto0_",
    "VetoSt11_": "Veto1_",
    "VetoSt20_": "Veto2_",
    "VetoSt21_": "Veto3_",
    }

    veto_variables = [
    # "time",
    # "peak",
    # "width",
    "charge",
    "raw_peak",
    "raw_charge",
    "baseline",
    "baseline_rms",
    "status"]

    calo_prefix_map = {
        "Calo0_": "CaloLo0_",
        "Calo1_": "CaloLo1_",
        "Calo2_": "CaloLo2_",
        "Calo3_": "CaloLo3_",
    }

    calo_variables = [
    "nMIP",
    "E_dep",
    "E_EM",
    # "time",
    "peak",
    "width",
    "charge",
    "raw_peak",
    "raw_charge",
    "baseline",
    "baseline_rms",
    "status"]

    for old_prefix, new_prefix in veto_prefix_map.items():
        for varname in veto_variables:
            df = check_df_and_apply_alias(df, old_prefix+varname, new_prefix+varname)  

    for old_prefix, new_prefix in calo_prefix_map.items():
        for varname in calo_variables:
            df = check_df_and_apply_alias(df, old_prefix+varname, new_prefix+varname)  

    return df


def alias_r0022_data(df: ROOT.RDataFrame) -> ROOT.RDataFrame:
    """
    The names of some variables changed with the introduction of prompt reco tag r0022 (in August 2025) 
    This function creates an alias which maps the new names back onto the old ones

    args: 
        df: ROOT.RDataframe - dataframe to apply aliases to
    
    returns:
        df: ROOT.RDataFrame - dataframe with the aliases applied (if required)
    """
    

    veto_prefix_map = {
    "VetoSt10_": "Veto10_",
    "VetoSt11_": "Veto11_",
    "VetoSt20_": "Veto20_",
    "VetoSt21_": "Veto21_",
    }

    veto_variables = [
    # "time",
    # "peak",
    # "width",
    "charge",
    "raw_peak",
    "raw_charge",
    "baseline",
    "baseline_rms",
    "status"]

    calo_prefix_map = {
        "Calo0_": "CaloLo0_",
        "Calo1_": "CaloLo1_",
        "Calo2_": "CaloLo2_",
        "Calo3_": "CaloLo3_",
    }

    calo_variables = [
    "nMIP",
    "E_dep",
    "E_EM",
    # "time",
    "peak",
    "width",
    "charge",
    "raw_peak",
    "raw_charge",
    "baseline",
    "baseline_rms",
    "status"]

    for old_prefix, new_prefix in veto_prefix_map.items():
        for varname in veto_variables:
            df = check_df_and_apply_alias(df, old_prefix+varname, new_prefix+varname)  

    for old_prefix, new_prefix in calo_prefix_map.items():
        for varname in calo_variables:
            df = check_df_and_apply_alias(df, old_prefix+varname, new_prefix+varname)  

    return df


def book_per_run_hists(df: ROOT.RDataFrame, run_number: int=None, lumi: float=None) -> List:
    """
    Function to book histograms which need to be evaluated for each run seperately
    
    args:
        df: ROOT.RDataFrame - the dataframe used to fill the histograms
        run_number: int (optional) - the run number to select for if given
        lumi: float (optional) - the luminosity of the run in fb; if given then weight histograms by /lumi

    returns:
       per_run_histos: [List] - List of histograms to be filled
    """

    if run_number:
        df_this_run = df.Filter(f"run == {run_number}", f"Run: {run_number}")
    else:
        df_this_run = df

    # For the lumi weighting, only do the RDF.Count if we need to for performance
    weight = 1
    nevents = 1
    if lumi:
        nevents = df_this_run.Count().GetValue()
        weight = lumi
        print(f"Info: Lumi = {lumi:.2f}")
        print(f"Info: Weight = {weight / nevents:.5f}")

    df_this_run = df_this_run.Define("weight", f"{weight / nevents}")

    GeV = 1000
    per_run_histos = []
    # per_run_histos.append(df_this_run.Histo1D(("CaloEnergyEMZoom", "CaloEnergyEMZoom;CaloEnergyEMZoom;Events", 1000, 0*GeV, 100*GeV), f"Calo_total_E_EM_fudged", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_x0", "Track_theta_x0;Track_theta_x0;Events", 50, -0.01, 0.01), f"Track_theta_x0", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_y0", "Track_theta_x0;Track_theta_x0;Events", 50, -0.005, 0.005), f"Track_theta_y0", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_x1", "Track_theta_x1;Track_theta_x1;Events", 50, -0.01, 0.01), f"Track_theta_x1", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_y1", "Track_theta_y1;Track_theta_y1;Events", 50, -0.005, 0.005), f"Track_theta_y1", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_y1", "Track_theta_y1;Track_theta_y1;Events", 50, -0.005, 0.005), f"Track_theta_y1", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_pz_charge0", "Track_pz_charge0;Track_pz_charge0;Events", 100, -500*GeV, 500*GeV), f"Track_pz_charge0", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_Chi2", "Track_Chi2;Track_Chi2;Events", 50, 0, 50), f"Track_Chi2", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_Chi2_2", "Track_Chi2_2;Track_Chi2_2;Events", 50, 0, 500), f"Track_Chi2", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_nDoF", "Track_nDoF;Track_nDoF;Events", 20, 0, 20), f"Track_nDoF", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_x0", "Track_x0;Track_x0;Events", 100, -100, 100), f"Track_x0", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_y0", "Track_y0;Track_y0;Events", 100, -100, 100), f"Track_y0", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_x0_pos", "Track_theta_x0_pos;Track_theta_x0_pos;Events", 50, -0.01, 0.01), f"Track_theta_x0_pos", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_x0_neg", "Track_theta_x0_neg;Track_theta_x0_neg;Events", 50, -0.01, 0.01), f"Track_theta_x0_neg", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_y0_pos", "Track_theta_y0_pos;Track_theta_y0_pos;Events", 50, -0.005, 0.005), f"Track_theta_y0_pos", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Track_theta_y0_neg", "Track_theta_y0_neg;Track_theta_y0_neg;Events", 50, -0.005, 0.005), f"Track_theta_y0_neg", "weight"))

    per_run_histos.append(df_this_run.Histo1D(("VetoNu0_charge", "VetoNu0_charge;VetoNu0_charge;Events", 50, 0.01, 300.0), f"VetoNu0_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("VetoNu1_charge", "VetoNu1_charge;VetoNu1_charge;Events", 50, 0.01, 300.0), f"VetoNu1_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("VetoSt10_charge", "VetoSt10_charge;VetoSt10_charge;Events", 50, 0.01, 300.0), f"VetoSt10_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("VetoSt20_charge", "VetoSt20_charge;VetoSt20_charge;Events", 50, 0.01, 300.0), f"VetoSt20_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("VetoSt21_charge", "VetoSt21_charge;VetoSt21_charge;Events", 50, 0.01, 300.0), f"VetoSt21_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Timing0_charge", "Timing0_charge;Timing0_charge;Events", 50, 1.0, 80.0), f"Timing0_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Timing1_charge", "Timing1_charge;Timing1_charge;Events", 50, 1.0, 80.0), f"Timing1_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Timing2_charge", "Timing2_charge;Timing2_charge;Events", 50, 1.0, 80.0), f"Timing2_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Timing3_charge", "Timing3_charge;Timing3_charge;Events", 50, 1.0, 80.0), f"Timing3_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Preshower0_charge", "Preshower0_charge;Preshower0_charge;Events", 50, 0.01, 15.0), f"Preshower0_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Preshower1_charge", "Preshower1_charge;Preshower1_charge;Events", 50, 0.01, 15.0), f"Preshower1_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Calo0_charge", "Calo0_charge;Calo0_charge;Events", 100, 0.01, 4.0), f"Calo0_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Calo1_charge", "Calo1_charge;Calo1_charge;Events", 100, 0.01, 4.0), f"Calo1_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Calo2_charge", "Calo2_charge;Calo2_charge;Events", 100, 0.01, 4.0), f"Calo2_charge", "weight"))
    per_run_histos.append(df_this_run.Histo1D(("Calo3_charge", "Calo3_charge;Calo3_charge;Events", 100, 0.01, 4.0), f"Calo3_charge", "weight"))
        
    return per_run_histos


def book_yield_hists(df: ROOT.RDataFrame, run_numbers: List[int]) -> List:

    yield_hists = []
    runs = list(run_numbers)

    nruns = int(max(runs) - min(runs) + 1)
    rmin =  min(runs)
    rmax =  max(runs) + 1

    yield_hists.append(df.Histo1D(("Yield", "Yield;Yield;Events", nruns, rmin, rmax), "run"))
    yield_hists.append(df.Histo1D(("TrkYield", "TrkYield;TrkYield;Events", nruns, rmin, rmax), "run", "NTracks"))
    yield_hists.append(df.Histo1D(("PosTrkYield", "PosTrkYield;PosTrkYield;Events", nruns, rmin, rmax), "run", "NPosTracks"))
    yield_hists.append(df.Histo1D(("NegTrkYield", "NegTrkYield;NegTrkYield;Events", nruns, rmin, rmax), "run", "NNegTracks"))
    
    yield_hists.append(df.Histo1D(("GoodTrkYield", "GoodTrkYield;GoodTrkYield;Events", nruns, rmin, rmax), "run", "NGoodTracks"))
    yield_hists.append(df.Histo1D(("GoodPosTrkYield", "GoodPosTrkYield;GoodPosTrkYield;Events", nruns, rmin, rmax), "run", "NGoodPosTracks"))
    yield_hists.append(df.Histo1D(("GoodNegTrkYield", "GoodNegTrkYield;GoodNegTrkYield;Events", nruns, rmin, rmax), "run", "NGoodNegTracks"))
    return yield_hists


def build_dataframe(file_list: List[str], tree: str='nt') -> ROOT.RDataFrame:
    """
    Function which constructs a ROOT RDataFrame from file(s) in `file_list`
    Applys the neccessary column definitions, aliases and data quality cuts

    args:
        file_list: List[str] - list of files to load
        tree: str - the name of the tree in the NTuples to read (default='nt')

    returns:
        df: ROOT.RDataFrame - the filtered dataframe with columns defined
    """

    df = ROOT.RDataFrame(tree, file_list)
    # ROOT.RDF.Experimental.AddProgressBar(df)

    #* Apply aliases to map p0012 variable names to their old ones
    # df = alias_p0012_data(df)
    df = alias_r0022_data(df)

    #* Allow shorter use of vecops functions in strings
    #* e.g. DeltaPhi rather than ROOT::VecOps::DeltaPhi 
    ROOT.gInterpreter.Declare("using namespace ROOT::VecOps;")

    #* C++ defines (must not rely on anything defined below)
    ROOT.gInterpreter.Declare('#include "RDFDefines.h"')

    #* Data quality cuts
    good_times_cut_str = make_good_times_cut(args.grl_path)           # Select for the periods of stable running
    df = df.Define("GoodTimes", good_times_cut_str)
    df = df.Filter("GoodTimes", "Good times")

    # print("good_times_cut_str", good_times_cut_str)
    
    exlcuded_times_cut_str = make_excluded_times_cut(args.grl_path)   # Some runs have certain time periods excluded. These periods are recorded in the GRL json files.
    if exlcuded_times_cut_str != "":
        df = df.Define("ExcludedTimes", exlcuded_times_cut_str)
        df = df.Filter("!ExcludedTimes", "Excluded times")
    
    # print("exlcuded_times_cut_str", exlcuded_times_cut_str)

    df = df.Filter("(Timing0_status & 4) == 0 && (Timing1_status & 4) == 0 && (Timing2_status & 4) == 0 && (Timing3_status & 4) == 0 ", "No timing saturation")
    df = df.Filter("distanceToCollidingBCID == 0", "Colliding") #! Commented out due to buggy nature in p0011/p0012. Remove this if running over 2022-2023 or the new 2024 data when it becomes available
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

    return df


def main(args: argparse.Namespace) -> None:

    #* Sanity check, if both these flags are set then no histograms will be made
    if args.skip_per_run and args.skip_yields:
        print("Warning: `do_yields` and `do_per_run` args set to False. Will not make any histograms.")

    #* Enable multithreading
    ROOT.ROOT.EnableImplicitMT(args.nCPUS)

    #* Parse input files
    file_list = get_per_run_filelists(args.input_files)

    if len(file_list) <= 0:
        print("Error: Found no files to run over")
        return 1

    print(f"Info: Running over {len(file_list)} files")

    #* Construct dataframes
    df_for_yields = build_dataframe(file_list)

    #* Print cutflow
    print("\nInfo: Cutflow Report:")
    cutReport = df_for_yields.Report()
    cutReport.Print()

    run_numbers = set(np.array(df_for_yields.AsNumpy(["run"])["run"]))
    
    print(run_numbers)

    yield_hists = book_yield_hists(df_for_yields, run_numbers)

    #* Get lumi dict
    lumi_dict = {}
    lumi_dict = get_run_number_lumi_dict(args.grl_path)
    total_lumi = None if not lumi_dict else sum(lumi_dict.values())
    print(f"Total luminosity: {sum(lumi_dict.values()):.3f} /fb")

    #* Get a list of available run numbers
    print("Info: Found the following run numbers: ")
    per_run_hists = {}
    for number in run_numbers:
        print(f"   - {int(number)}")
        run_file_list = []
        for fpath in file_list:
            if f"{number}" in fpath:
                run_file_list.append(fpath)
        
        #* Book per run histograms 
        if not args.skip_per_run and not args.do_not_split:
            df_for_run = build_dataframe(run_file_list)
            run_hists = book_per_run_hists(df_for_run, number, lumi=lumi_dict.get(number, None))
            per_run_hists[number] = run_hists

    #* Book the combined kinematic histograms if requested
    if args.do_not_split:
        per_run_hists['All'] = book_per_run_hists(df_for_yields, lumi=total_lumi)

    #* Make output file (and output directory if needs be)
    output_dir = os.path.dirname(args.output_file)
    os.makedirs(os.path.abspath(output_dir), exist_ok=True)
    file = ROOT.TFile(args.output_file, "RECREATE")
    print(f"Info: Writing output to {args.output_file}")

    #* For each run write the per run histograms and save them to root file subdirectory
    if not args.skip_per_run:
        for run, hists in tqdm(per_run_hists.items(), desc="Info: Filling per run hists: "):
            subdir1 = file.mkdir(f"Main__{run}")
            subdir1.cd()
            subdir2 = subdir1.mkdir(f"PerRun")
            subdir2.cd()
            for h in hists:
                h.Write()
            file.cd()
    else:
        print("Info: Skipping per run hists")

    if not args.skip_yields:
        subdir3 = file.mkdir("Main")
        subdir3.cd()
        subdir4 = subdir3.mkdir("Yields")
        subdir4.cd()
        for h in tqdm(yield_hists, desc="Info: Filling  yield hists: "):
            h.Write()
        file.cd
    else:
        print("Info: Skipping yield hists")

    file.Close()
    print(f"Info: Wrote output to {args.output_file}")


    #* Print cutflow
    print("\nInfo: Cutflow Report:")
    cutReport = df_for_yields.Report()
    cutReport.Print()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_files", "-i", nargs='+', help = "Imput sample list yaml file(s)", required=True)
    parser.add_argument("--output_file", "-o", type=str, default="output.root", help = "Output file for the results")
    parser.add_argument("--grl_path", "-g", type=str, default="/cvmfs/faser.cern.ch/repo/sw/runlist/v8", help = "Path to directory containing GRL files in the .json format")
    parser.add_argument("--nCPUS", "-j", type=int, default=0, help = "Number of CPU cores to use. Default is zero, i.e. all cores")
    parser.add_argument("--skip_yields", "-y", action="store_true", help= "If True, run the yield histograms")
    parser.add_argument("--skip_per_run", "-r", action="store_true", help= "If True, run the per run histograms")
    parser.add_argument("--differential", "-d", action="store_true", help= "If True, make per run histograms per fb^-1")
    parser.add_argument("--do_not_split", "-n", action="store_true", help= "If True, do not split the per run histograms by run. Make a total histogram of all runs instead.")
    args = parser.parse_args()

    for key in vars(args):
        print(f"\t {key:<30}: {getattr(args, key)}")


    main(args)