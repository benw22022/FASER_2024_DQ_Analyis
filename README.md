# FASER Data Quality Analyis

## Getting started 

Clone the repository:
```bash
git clone https://github.com/benw22022/FASER_2024_DQ_Analyis.git
```

The code has fairly minimal dependacies just needs `ROOT`, `numpy`, `pyaml` and `tqdm` plus access to `cvmfs` and `eos`

## Description

This code is a slimmed down version of [`FASERRDFAnalysis`](https://gitlab.cern.ch/tboeckh/FASERRDFAnalysis/-/tree/electronic-neutrino-analysis-2023?ref_type=heads
) adapted to be more flexible when it comes to the renaming of branches in the 2024 NTuples.

The `p0012` NTuples certain branches got renamed (as described on the [Twiki](https://twiki.cern.ch/twiki/bin/viewauth/FASER/PhysicsNtuples)). This code works around this by applying an alias to the branches in this files which maps them onto the old branch names.

The main script in this code is [`FASER_DQ_RDF.py`](https://github.com/benw22022/FASER_2024_DQ_Analyis/blob/main/FASER_DQ_RDF.py) which aims to produce the histograms neccessary for the [`data_quality.ipynb`](https://github.com/tobias-boeckh/elecnu/blob/main/elecnu/notebooks/data_quality.ipynb) notebook in the main `elecnu` code. 

`FASER_DQ_RDF.py` has the following arguments:
- `--input_files` (`-i`): A list of input files or a list of `yaml` files containing NTuple file paths. `yaml` files must contain the key `samples`
- `--output_file` (`-o`): Name of the output `ROOT` file (defaults to `output.root`)
- `--grl_path` (`-g`): Path to directory containing the good run lists in `json` format. These are required to apply cuts on excluded event times.
- `--skip_yields`: If flag is applied then skip the creation of the yield histograms
- `--skip_per_run`: If flag is applied then skip the creation of the per run histograms

*Important note:* You must only run over samples with the same branches, if you try and run over, for example, both `p0011` and `p0012` it will not work properly. You'll need to run the code seperately for `p0012` and for `p0011` and earlier samples.

Once you have done this you'll need to combine the files together using `hadd`, which will give you your complete per run data quality histogram file

For the yield histograms, extra care needs to be taken as these cannot be simply added and must instead be 'concatenated' together as they are binned in run rumber. A script, [`combine_yield_files.py`](https://github.com/benw22022/FASER_2024_DQ_Analyis/blob/main/combine_yield_files.py) is provided for doing this.
