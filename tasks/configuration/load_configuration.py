#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
from os.path import basename, join


class Configuration:
    threads = 33
    reference = "/mnt/raid/zparteka/hg38_reference/Homo_sapiens_assembly38.fasta"
    mapq = 30
    peak_quality = 0.01
    outnames = {"mapped": None, "mapped_only": None, "filtered": None, "nodup": None, "bigwig": None, "index:": None,
                "sorted": None,
                "peaks": None}

    def __init__(self, r1="/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R1_001.fastq.gz", r2="/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R2_001.fastq.gz"):
        self.r1 = r1
        self.r2 = r2
        self.create_outnames()
        # todo chnge this: my case - can be overwritten
        self.outdir = r1.split("fastq")[0] + "maps_output/"

        # todo add checking if value is None

    def create_outnames(self):
        base = basename(self.r1.split("_R1")[0])
        self.outnames["mapped"] = join(self.outdir, f"{base}.bam")
        self.outnames["mapped_only"] = join(self.outdir, f"{base}_mapped.bam")
        self.outnames["filtered"] = join(self.outdir, f"{base}_mapped_filtered.bam")
        self.outnames["bigwig"] = join(self.outdir, f"{base}.bw")
        self.outnames["nodup"] = join(self.outdir, f"{base}_mapped_filtered_nodup.bam")
        self.outnames["index"] = join(self.outdir, f"{base}_indexed.bam")
        self.outnames["sorted"] = join(self.outdir, f"{base}_sorted.bam")
        self.outnames["peaks"] = join(self.outdir, f"{base}_macs3")


def load():
    conf = Configuration()
    return conf