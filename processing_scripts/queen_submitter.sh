#!/bin/bash -l

#SBATCH --job-name=QUEEN
#SBATCH --output=./temp/queen-%j.out
#SBATCH --error=./temp/queen-%j.err
#SBATCH --partition=production

TASK="$1"
FUNC="$2"

cmd="python ./processing_scripts/queen.py $TASK $FUNC"

echo "$cmd"
eval "$cmd"

