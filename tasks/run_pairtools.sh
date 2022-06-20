#!/usr/bin/env bash
COMPRESS_PROGRAM=gzip

pairtools parse $MAPPED -c $CHROMOSOMES --add-columns mapq | pairtools sort --nproc ${THREADS} --memory ${MEM} --compress-program ${COMPRESS_PROGRAM}
                                                         --tmpdir ${TMPDIR} --output ${SORTED_PAIRS_PATH}