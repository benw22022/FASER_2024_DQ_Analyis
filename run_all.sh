#!/bin/bash

# mkdir -p output_2022/logs
mkdir -p output_2023/logs
mkdir -p output_2024/logs

# condor_submit DQ_2022.submit
condor_submit DQ_2023.submit
condor_submit DQ_2024.submit