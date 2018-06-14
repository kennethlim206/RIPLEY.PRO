#!/bin/bash -l

TASK="$1"
FUNC="$2"

cmd="python ./processing_scripts/burst.py $TASK $FUNC"

eval "$cmd"