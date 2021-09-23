#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
from os import makedirs
from os.path import basename, join, isdir, dirname
import pickle

# Put your samples here (pair-end fastq files)

samples_yoruban = {
    "gm19238_CTCF": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_i/fastq/GM19238_CTCF_I_part2_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_i/fastq/GM19238_CTCF_I_part2_R2.fastq.gz"),
                     (
                         "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_ii/fastq/GM19238_CTCF_II_R1.fastq.gz",
                         "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_ii/fastq/GM19238_CTCF_II_R2.fastq.gz")],
    "gm19239_CTCF": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/ctcf_i/fastq/GM19239_CTCF_I_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/ctcf_i/fastq/GM19239_CTCF_I_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/ctcf_ii/fastq/GM19239_CTCF_II_S2_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/ctcf_ii/fastq/GM19239_CTCF_II_S2_R2.fastq.gz")],
    "gm19240_CTCF": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/ctcf_i/fastq/GM19240_CTCF_I_S5_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/ctcf_i/fastq/GM19240_CTCF_I_S5_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/ctcf_ii/fastq/GM19240_CTCF_II_S6_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/ctcf_ii/fastq/GM19240_CTCF_II_S6_R2.fastq.gz")],
    "gm19238_smc1": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/smc1_i/fastq/GM19238_Smc1_I_part2_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/smc1_i/fastq/GM19238_Smc1_I_part2_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/smc1_ii/fastq/GM19238_Smc1_II_S1_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/smc1_ii/fastq/GM19238_Smc1_II_S1_R2.fastq.gz")],
    "gm19239_smc1": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/smc1_i/fastq/GM19239_Smc1_I_S3_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/smc1_i/fastq/GM19239_Smc1_I_S3_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/smc1_ii/fastq/GM19239_Smc1_II_S4_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/smc1_ii/fastq/GM19239_Smc1_II_S4_R2.fastq.gz")],
    "gm19240_smc1": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/smc1_i/fastq/GM19240_Smc1_I_S7_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/smc1_i/fastq/GM19240_Smc1_I_S7_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/smc1_ii/fastq/GM19240_Smc1_II_S8_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/smc1_ii/fastq/GM19240_Smc1_II_S8_R2.fastq.gz")]}
# on team-arwena
samples_chinese = {
    "hg00512_CTCF": [("/mnt/raid/zparteka/hichip/chinese/HG00512/ctcf_i/fastq/HG00512_CTCF_I_S2_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00512/ctcf_i/fastq/HG00512_CTCF_I_S2_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip/chinese/HG00512/ctcf_ii/fastq/HG00512_CTCF_II_S8_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00512/ctcf_ii/fastq/HG00512_CTCF_II_S8_R2.fastq.gz")],
    "hg00513_CTCF": [("/mnt/raid/zparteka/hichip/chinese/HG00513/ctcf_i/fastq/HG00513_CTCF_I_S3_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00513/ctcf_i/fastq/HG00513_CTCF_I_S3_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip/chinese/HG00513/ctcf_ii/fastq/HG00513_CTCF_II_S9_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00513/ctcf_ii/fastq/HG00513_CTCF_II_S9_R2.fastq.gz")],
    "hg00514_CTCF": [("/mnt/raid/zparteka/hichip/chinese/HG00514/ctcf_i/fastq/HG00514_CTCF_I_S4_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00514/ctcf_i/fastq/HG00514_CTCF_I_S4_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip/chinese/HG00514/ctcf_ii/fastq/HG00514_CTCF_II_S10_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00514/ctcf_ii/fastq/HG00514_CTCF_II_S10_R2.fastq.gz")]
}
# on team-bilbo
samples_puerto = {
    "hg00731_CTCF": [("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_i/fastq/HG00731_CTCF_I_S5_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_i/fastq/HG00731_CTCF_I_S5_R2.fastq.gz"),
                     (
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_ii/fastq/HG00731_CTCF_II_S11_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_ii/fastq/HG00731_CTCF_II_S11_R2.fastq.gz")],
    "hg00732_CTCF": [("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_i/fastq/HG00732_CTCF_I_S6_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_i/fastq/HG00732_CTCF_I_S6_R2.fastq.gz"),
                     (
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_ii/fastq/HG00732_CTCF_II_S12_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_ii/fastq/HG00732_CTCF_II_S12_R2.fastq.gz")],
    "hg00733_CTCF": [("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_i/fastq/HG00733_CTCF_I_S7_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_i/fastq/HG00733_CTCF_I_S7_R2.fastq.gz"),
                     (
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_ii/fastq/HG00733_CTCF_II_S12_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_ii/fastq/HG00733_CTCF_II_S12_R2.fastq.gz")]
}
trial_samples = {"ko": [("/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R1_001.fastq.gz",
                         "/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R2_001.fastq.gz"),
                        ("/mnt/raid/zparteka/natalia_uva/ko2/fastq/KO2_S2_L001_R1_001.fastq.gz",
                         "/mnt/raid/zparteka/natalia_uva/ko2/fastq/KO2_S2_L001_R2_001.fastq.gz")]}

samples = samples_yoruban

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
        # todo change this: my case - can be overwritten
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
        self.outnames["maps"] = join(self.outdir,
                                     f"MAPS_output/{self.maps_dataset}_current/{self.maps_dataset}.5k.2.sig3Dinteractions.bedpe")

    def dumps(self):
        c = {"r1": self.r1, "r2": self.r2}

        return c


def loads(p):
    c = Configuration(p["r1"], p["r2"])
    return c
