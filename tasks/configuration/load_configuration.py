#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
from os import makedirs
from os.path import basename, join, isdir
import pickle


class Configuration:
    threads = 33
    reference = "/mnt/raid/zparteka/hg38_reference/Homo_sapiens_assembly38.fasta"
    mapq = 30
    peak_quality = 0.01
    outnames = {"mapped": None, "mapped_only": None, "filtered": None, "nodup": None, "bigwig": None, "index:": None,
                "sorted": None,
                "peaks": None}

    def __init__(self, r1, r2):
        self.r1 = r1
        self.r2 = r2
        # todo change this: my case - can be overwritten
        self.outdir = r1.split("fastq")[0] + "luigi_seq_output/"
        if not isdir(self.outdir):
            makedirs(self.outdir)

        self.create_outnames()

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
    print(p.keys())
    c = Configuration(p["r1"], p["r2"])
    return c
