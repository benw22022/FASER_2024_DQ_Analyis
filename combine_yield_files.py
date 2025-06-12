"""
Yield File Combiner
_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_

Script for concatenating histograms binned in run number
Allows for yield histograms from different run periods
to be combined into a single file without having to
run the RDF analysis over every file

Usage:

    python3 combine_yield_files.py -i  <file1> <file2> <file3> -o <output file>
"""

import os
import uproot
import argparse
import numpy as np


def main(args: argparse.Namespace) -> None:

    # Extract histograms from input files
    unmerged_histograms = {}
    for fpath in args.input_files:
        file = uproot.open(fpath)
        data = file["Main"]["Yields"]

        for hist_name, histogram in data.items():
            if not unmerged_histograms.get(hist_name, False):
                unmerged_histograms[hist_name] = [] 
            unmerged_histograms[hist_name].append(histogram) 

    print(f"Info: Found {len(unmerged_histograms.keys())} histograms in {len(args.input_files)} files")

    # Create output file, make output directory if neccessary
    output_dir = os.path.dirname(args.output_file)
    if output_dir: os.makedirs(output_dir, exist_ok=True)
    output_file = uproot.recreate(args.output_file)
    output_file.mkdir("Main/Yields")

    # Create a new histogram filled with zeros covering the full run number range
    # Then loop though each histogram, work out where the bins of which start in the new histogram
    for h_idx, (hist_name, hists_to_concat) in enumerate(unmerged_histograms.items()):
        npy_hists = [h.to_numpy() for h in hists_to_concat]

        edges = [e for _, e in npy_hists]        
        concat_edges = np.concatenate((edges))
        
        lower_edge = np.amin(concat_edges)
        upper_edge = np.amax(concat_edges)
        new_edges = np.linspace(lower_edge, upper_edge, int(upper_edge-lower_edge+1))
        
        new_histogram_values, _ = np.histogram(np.zeros(len(new_edges)-1), new_edges)

        for npy_hist in npy_hists:

            # Work out where this histogram starts
            start_idx = np.where(new_edges == np.amin(npy_hist[1]))[0]
            
            # Validate the binning, `start_idx` is a numpy array containing exactly one value
            if len(start_idx) != 1:
                if len(start_idx) > 1:
                    print("Error: Found a histogram with repeated bin edges - something is not right")
                else:
                    print("Error: Tried to concatenate a histogram but it's bin edges are non-integer - this script is for merging histograms binned by run number")
                raise ValueError(f"Invalid histogram binning for histogram {hist_name} in file {args.input_files[h_idx]}")

            for i, bin_value in enumerate(npy_hist[0]):
                new_histogram_values[start_idx + i] = bin_value

        # Write the new concatenated histogram to file
        output_file[f"Main/Yields/{hist_name.split(';')[0]}"] = new_histogram_values, new_edges
    
    print(f"Info: Done. Combined output file written to {args.output_file}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_files", "-i", nargs='+', help = "List of yield files to join", required=True)
    parser.add_argument("--output_file", "-o", type=str, default="combined_yields.root", help = "Output file for the results")
    args = parser.parse_args()
    
    main(args)
