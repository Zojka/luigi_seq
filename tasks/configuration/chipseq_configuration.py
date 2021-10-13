#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
from os import makedirs
from os.path import basename, join, isdir, dirname

# chipseq analysis configuration - if using input - each sample has to have matching input in input dict


chips = {"wt_k9ac": [],
         "ko_k9ac": [],
         "wt_sirt6": [],
         "ko_sirt6": [],
         "wt_k56ac": [],
         "ko_k56ac:": []}

input = {"wt_k9ac": [[path1_r1, path1_r2], [path2_r1, path2_r2], ...],
         "ko_k9ac": [],
         "wt_sirt6": [],
         "ko_sirt6": [],
         "wt_k56ac": [],
         "ko_k56ac:": []}


class Configuration:
    threads = 33
    reference = "/mnt/raid/zparteka/hg38_reference/Homo_sapiens_assembly38.fasta"
    mapq = 30
    peak_quality = 0.01
    outnames = {"mapped": None, "mapped_only": None, "filtered": None, "nodup": None, "bigwig": None, "index:": None,
                "sorted": None, "peaks": None}

    def __init__(self, r1, r2):
        self.r1 = r1
        self.r2 = r2
        self.outdir = r1.split("fastq")[0] + "luigi_seq_output/"
        if not isdir(self.outdir):
            makedirs(self.outdir)
        self.maps_dataset = basename(r1).split("_R")[0]
        self.fastq_dir = dirname(r1)
        self.bwa_index = "/mnt/raid/zparteka/hg38_reference/Homo_sapiens_assembly38.fasta"
        self.create_outnames()
        self.narrow_peak = self.outnames["peaks"] + "_peaks.narrowPeak"

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

    def dumps(self):
        c = {"r1": self.r1, "r2": self.r2}

        return c


def loads(p):
    c = Configuration(p["r1"], p["r2"])
    return c
