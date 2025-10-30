#!/bin/bash

export EOS_MGM_URL=root://eospublic.cern.ch
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc11-opt/setup.sh
run=$1
output_dir=$2

source_dir=/home/ppd/bewilson/work/FASER_2024_DQ_Analyis/

cp ${source_dir}/RDFDefines.h .
python3 ${source_dir}/FASER_DQ_RDF.py $run --output_file_dir $output_dir --input_file_list_dir ${source_dir}/faser_filelists --histograms ${source_dir}/histograms