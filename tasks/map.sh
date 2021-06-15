#!/usr/bin/env bash

bwa mem -SP5M -v 0 -t${THREADS} ${REFERENCE} ${R1} ${R2} | samtools view -bhS - > ${OUTNAME_MAPPED}