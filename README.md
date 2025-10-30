# FASER Data Quality Analyis

## Getting started 

Clone the repository:
```bash
git clone https://github.com/benw22022/FASER_2024_DQ_Analyis.git
```

The code has fairly minimal dependacies, just needs access to `cvmfs`

## Description

This code is a slimmed down version of [`FASERRDFAnalysis`](https://gitlab.cern.ch/tboeckh/FASERRDFAnalysis/-/tree/electronic-neutrino-analysis-2023?ref_type=heads
); adapted to be more flexible when it comes to the renaming of branches in the 2024 NTuples.

The code makes use of `pyROOT` [`RDataFrames`](https://root.cern/doc/v632/classROOT_1_1RDataFrame.html) to quickly analyse large quantities of data in python

The main script in this code is [`FASER_DQ_RDF.py`](https://github.com/benw22022/FASER_2024_DQ_Analyis/blob/main/FASER_DQ_RDF.py), this code runs over one run at a time to generate a root file containing the track yields and various distributions

`FASER_DQ_RDF.py` has the following arguments:
- `run`: The run number (required)
- `--input_files` (`-i`): Path to the directory containing `txt` files listing the available file paths (the paths to the latest files at time of writing for 2022-2024 are in `/faser_filelists`)
- `--histograms` (`-c`): Path to directory containing the `.yaml` files which define the histograms
- `--output_file_dir` (`-o`): Path to the output directory which will store the results. The output files themselves are named `<run_number>.root`
- `--grl_path` (`-g`): Path to directory containing the good run lists in `json` format. These are required to apply cuts on excluded event times.

There are some quirks with the code, like the aliasing function which allows the code to run over 2022/2023 data where some variables didn't exists, such as the Hi/Lo gain calorimeter variables and the Veto 11 station.
In this case I alias the variables which don't exist to the closest match. E.g. map `CaloHi0_charge` and `CaloLo0_charge` get aliased to `Calo0_charge`.

## How to run

This code is optimised to run using a `htcondor` cluster where you submit one job per run
To submit jobs you need three files:
- A bash executable like `run_DQ.sh`
- A file containing the args to be given to each job (e.g. `args_2024.txt`)
- A submission file like `DQ_2024.submit`

The submission file looks something like this
```bash
universe = vanilla                                                 # The enviroment
executable = run_DQ.sh                                             # The executable to be run on the node
arguments = $(runs) $(output_dir)                                  # Specifies the arguments
output = $(output_dir)/logs/job_$(runs)_$(Process).out             # Path to the log file containing the stdout from the job
error = $(output_dir)/logs/job_$(runs)_$(Process).err              # Path to the log file containing the stderr from the job
log = $(output_dir)/logs/job_$(runs)_$(Process).log                # Path to the file containing condor submission log
request_cpus = 4                                                   # Number of CPUs to request
+JobFlavour = "longlunch"                                          # The maximum ('wall') time of the job. Job will be killed after this time. See [batch doc](https://batchdocs.web.cern.ch/local/submit.html#job-flavours) for more info on available job flavours.
on_exit_remove   = (ExitBySignal == False) && (ExitCode == 0)      # 
max_retries      = 3                                               # These three magic lines will get the job to restart if it fails (useful for `eos` hiccups)
requirements     = (Machine =!= split(LastRemoteHost, "@")[1])     #
queue runs, output_dir from args_2024.txt                          # This tells condor to read this arguments file line by line. One job per line. Arguments seperated by spaces.
```


The arguments file looks something like this:
```bash
14587 /home/ppd/bewilson/FASER_2024_DQ_Analyis/output_2024
14588 /home/ppd/bewilson/FASER_2024_DQ_Analyis/output_2024
14589 /home/ppd/bewilson/FASER_2024_DQ_Analyis/output_2024
14590 /home/ppd/bewilson/FASER_2024_DQ_Analyis/output_2024
14593 /home/ppd/bewilson/FASER_2024_DQ_Analyis/output_2024
14597 /home/ppd/bewilson/FASER_2024_DQ_Analyis/output_2024
14618 /home/ppd/bewilson/FASER_2024_DQ_Analyis/output_2024
14644 /home/ppd/bewilson/FASER_2024_DQ_Analyis/output_2024
...
```

You'll want to do a search and replace the output directory path to the *absolute* path on your system. 

Finally the executable file looks like this

```bash
#!/bin/bash

export EOS_MGM_URL=root://eospublic.cern.ch                                              # Required by xROOTD
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc11-opt/setup.sh                # Sets up the environment using the LCG_107 software stack (basically gives us a recent ROOT and python release)
run=$1                                                                                   # The first command line arg provided by the condor submission file
output_dir=$2                                                                            # The second command line arg provided by the condor submission file

cp /home/ppd/bewilson/FASER_2024_DQ_Analyis/RDFDefines.h .                               # This is a C++ header which defines some magic shorthand which we can make use of in python
python3 /home/ppd/bewilson/FASER_2024_DQ_Analyis/FASER_DQ_RDF.py $run -o $output_dir     # This is where we actually execute the script with the supplied arguments
```

**Again you'll need to replace the absolute paths here with your own paths!! There is probably a smart way to fix this so that you don't need to use absolute paths everywhere but I've found it's the most reliable way to make sure that the output to your jobs goes where you expect!**

Before submitting the to condor you should test your job locally first to see if it works, e.g. if you run

```bash
python3 FASER_DQ_RDF_improved.py 14797 -o test
```

you should get a file called `test/14797.root`. You'll need to run the `LCG` setup first with

```bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc11-opt/setup.sh
```

but this only needs to be done once per terminal session.

I find that on non-lxplus machines I get prompted for the password to my grid certificate when running the code in a terminal like this. If you the code doesn't run, it might be because you need one (I can't remember what the error message is if this happens). You can obtain a grid certificate from [here](https://ca.cern.ch/ca/user/Request.aspx?template=EE2User) and follow the instructions in this [evernote](https://lite.evernote.com/note/c7bc28df-e637-4625-b6a5-5b97d0b3a93a) on how to install it on your machine (this guide was written a while ago for ATLAS users at Manchster you just need to follow the bit from where it says "*Get the certificates working - lxplus*" - ignore the stuff about VO membership).

Once you are happy with everything, then you can go ahead and submit the jobs. But first...

**IMPORTANT NOTE** The jobs will get stuck if the output directory (including the logs folder) does not exists before submitting you need to run!! i.e. 

```bash
mkdir -p output_2024/logs
```

...and then you can submit the jobs using 

```bash
condor_submit DQ_2024.submit
```

Again, probably a smart way to do this so that it does the directory creation automatically - I'm just a bit too lazy to fix this.

You can watch the progress of your jobs using the command `condor_q`. You can `ssh` to a job to check on it's progress with `condor_ssh_to_job <jobID>` and look at the `condor_stdout` file using `vi`.

If the jobs are stuck on `hold` you can check the reason why by `condor_ssh_to_job <jobID>` (probably because you've forgotton to create the output directory). If possible, you can try and fix the issue causing the jobs to get stuck and then get them running again with the command `condor_release <your-username>`.

If for some reason you want to cancel all your running/queued jobs you can kill them all with `condor_rm <your-username>` (or `condor_rm <jobID>` if it is just the one job you want to kill)

For more detailed info on how to use `htcondor` refer to the CERN [`batchdocs`](https://batchdocs.web.cern.ch/index.html) webpage


## Booking histograms
To help keep the python code organised and to avoid the need to manage python paths, the histograms for different variables are defined using `yaml` config files. A config file needs the following structure:

```yaml
histograms:

    # A unique key for this histogram config
    my_histogram:                                
        # Name of column in dataframe to plot. Must be defined in RDF at runtime
        name: my_variable 
        # Pretty name for plotting (optional)
        latex: My Variable [Units]
        # Number of bins
        nbins: 50
        # Lower bin edge
        min: 0
        # Upper bin edge
        max: 100
        # Factor to multiply values by before histograming. Useful for unit conversions (optional)
        unit_scale: 0.001
        # Cut to apply before histograming this variable (optional)
        cut:
            # The cut expression (must be parsable by PyROOT RDF)
            expression: my_other_variable > 10
            # A name to give the cut (optional)
            name: This > 10 Units

```


## xRootD

Reading from `eos` over the default protocol (`FUSE`) can be pretty flakey sometimes cause code to fail for no apparent reason

Instead we can make use of the `xRootD` protocol which is much more reliable. To exploit `xRootD` we make the following changes:
1. Append `root://eospublic.cern.ch/` to the filepaths to our root files - which you can see in `faser_filelists/2024_filelist.txt`
2. Define this enviroment variable `export EOS_MGM_URL=root://eospublic.cern.ch`. This is done in the `run_DQ.sh` submission script, but you'll probably want to add this to your `.bashrc` too.

One of this nice things about using `xRootD` too is that you can read files remotely on machines and clusters without access to `eos`. All you need is access to `cvmfs` so that you can source an `LCG` release.
