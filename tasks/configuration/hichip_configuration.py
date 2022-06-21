#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
from os import makedirs
from os.path import basename, join, isdir, dirname

"""genome sizes
hs: 2.7e9
mm: 1.87e9
ce: 9e7
dm: 1.2e8

"""

sample_arima_hichip_shallow = {"sample": [("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_shallow/fastq/KJ_Swift_1_R1.fastq.gz",
                                   "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_shallow/fastq/KJ_Swift_1_R2.fastq.gz")]}
sample_arima_hichip_deep = {"sample": [("/mnt/raid/zparteka/cohesin_hichip_project/karolina_arima/raw_data/fastq/KJ_Swift_1_S12_R1.fastq.gz",
                                        "/mnt/raid/zparteka/cohesin_hichip_project/karolina_arima/raw_data/fastq/KJ_Swift_1_S12_R2.fastq.gz")]}
samples = sample_arima_hichip_deep

class Configuration:
    threads = 33
    # reference = "/mnt/raid/zparteka/mm10_genome/bwa/mm10.fa"
    reference = "/mnt/raid/zparteka/hg38_reference/Homo_sapiens_assembly38.fasta"
    mapq = 30
    peak_quality = 0.05
    genome_size = "hs"

    def __init__(self, r1, r2):
        self.r1 = r1
        self.r2 = r2
        self.outdir = r1.split("fastq")[0] + "luigi_seq_output/"
        if not isdir(self.outdir):
            makedirs(self.outdir)
        self.maps_dataset = basename(r1).split("_R")[0]
        self.fastq_dir = dirname(r1)
        # self.bwa_index = "/mnt/raid/zparteka/mm10_genome/bwa/mm10.fa"
        self.bwa_index = "/mnt/raid/zparteka/hg38_reference/Homo_sapiens_assembly38.fasta"
        self.outnames = {}
        self.create_outnames()
        self.narrow_peak = self.outnames["peaks"] + "_peaks.narrowPeak"
        self.chromosomes = "/mnt/raid/zparteka/hg38_reference/hg38_chrom.sizes"
        self.memory = "48G"

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
        self.outnames["paired"] = join(self.outdir, f"{base}_paired")
        self.outnames["final_bam"] = join(self.outdir, f"{base}_final.bam")
        self.outnames["maps"] = join(self.outdir,
                                     f"MAPS_output/{self.maps_dataset}_current/{self.maps_dataset}.5k.2.sig3Dinteractions.bedpe")
        self.outnames["dups_stats"] = join(self.outdir, f"{base}_dups_stats.txt")

    def dumps(self):
        c = {"r1": self.r1, "r2": self.r2}

        return c


def loads(p):
    c = Configuration(p["r1"], p["r2"])
    return c
