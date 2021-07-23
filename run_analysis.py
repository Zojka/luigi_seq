#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import local
from tasks.configuration.load_configuration import Configuration
from tasks.maps_task import RunMapsPulledReplicates
from os.path import basename, dirname, join, isdir
from pathlib import Path
from os import makedirs

# todo run analysis on multiple hichip samples

samples_yoruban = {"gm19238_CTCF": [(
    "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_i/fastq/GM19238_CTCF_I_part2_R1.fastq.gz",
    "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_i/fastq/GM19238_CTCF_I_part2_R2.fastq.gz"), (
    "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_ii/fastq/GM19238_CTCF_II_R1.fastq.gz",
    "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_ii/fastq/GM19238_CTCF_II_R2.fastq.gz")]}
samples_chinese = {}
samples_puerto = {}
trial_samples = {"ko": [("/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R1_001.fastq.gz",
                         "/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R2_001.fastq.gz"),
                        ("/mnt/raid/zparteka/natalia_uva/ko2/fastq/KO2_S2_L001_R1_001.fastq.gz",
                         "/mnt/raid/zparteka/natalia_uva/ko2/fastq/KO2_S2_L001_R2_001.fastq.gz")]}


class RunAnalysis(luigi.WrapperTask):

    def requires(self):

        samples = samples_yoruban

        for sam in samples.keys():
            sample = samples[sam]
            folder = join(Path(dirname(sample[0][0])).parent.parent.absolute(), f"{sam}_pulled/fastq/")
            if not isdir(folder):
                makedirs(folder)
            out_r1 = join(folder, f"{sam}_pulled_R1.fastq.gz")
            out_r2 = join(folder, f"{sam}_pulled_R2.fastq.gz")
            #
            # cat = local["cat"]
            # (cat[sample[0][0], sample[1][0]] > out_r1)()
            # (cat[sample[0][1], sample[1][1]] > out_r2)()

            sample.append((out_r1, out_r2))

            yield RunMapsPulledReplicates(sample)
