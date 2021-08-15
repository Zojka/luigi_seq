#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import local
from tasks.configuration.load_configuration import Configuration
from tasks.maps_task import RunMapsPulledReplicates, CalculateCoverage
from os.path import basename, dirname, join, isdir, isfile
from pathlib import Path
from os import makedirs

# on bilbo
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
                     ("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_ii/fastq/HG00731_CTCF_II_S11_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_ii/fastq/HG00731_CTCF_II_S11_R2.fastq.gz")],
    "hg00732_CTCF": [("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_i/fastq/HG00732_CTCF_I_S6_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_i/fastq/HG00732_CTCF_I_S6_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_ii/fastq/HG00732_CTCF_II_S12_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_ii/fastq/HG00732_CTCF_II_S12_R2.fastq.gz")],
    "hg00733_CTCF": [("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_i/fastq/HG00733_CTCF_I_S7_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_i/fastq/HG00733_CTCF_I_S7_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_ii/fastq/HG00733_CTCF_II_S12_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_ii/fastq/HG00733_CTCF_II_S12_R2.fastq.gz")]
}
trial_samples = {"ko": [("/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R1_001.fastq.gz",
                         "/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R2_001.fastq.gz"),
                        ("/mnt/raid/zparteka/natalia_uva/ko2/fastq/KO2_S2_L001_R1_001.fastq.gz",
                         "/mnt/raid/zparteka/natalia_uva/ko2/fastq/KO2_S2_L001_R2_001.fastq.gz")]}


class RunAnalysis(luigi.WrapperTask):

    def requires(self):

        samples = samples_puerto

        for sam in samples.keys():
            sample = samples[sam]
            folder = join(Path(dirname(sample[0][0])).parent.parent.absolute(), f"{sam}_pulled/fastq/")
            if not isdir(folder):
                makedirs(folder)
            out_r1 = join(folder, f"{sam}_pulled_R1.fastq.gz")
            out_r2 = join(folder, f"{sam}_pulled_R2.fastq.gz")

            if isfile(out_r2) and isfile(out_r1):
                pass
            else:
                cat = local["cat"]
                (cat[sample[0][0], sample[1][0]] > out_r1)()
                (cat[sample[0][1], sample[1][1]] > out_r2)()

            if (out_r1, out_r2) not in sample:
                sample.append((out_r1, out_r2))
            print(sample)
            yield CalculateCoverage(sample)
