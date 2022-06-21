#!/usr/bin/env bash
COMPRESS_PROGRAM=gzip

pairtools parse $MAPPED -c $CHROMOSOMES --add-columns mapq | pairtools sort --nproc 5 --tmpdir ${TMPDIR} --memory 8G --output ${SORTED_PAIRS_PATH}