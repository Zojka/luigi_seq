#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import local
from tasks.configuration.load_configuration import Configuration
from tasks.maps_task import RunMapsPulledReplicates
# todo run analysis on multiple hichip samples

# todo add config files



class RunAnalysis(luigi.WrapperTask):

    def requires(self):

        # todo build a dictionary with samples (find? glob?)

        samples = {"ko": [("/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R1_001.fastq.gz",
                           "/mnt/raid/zparteka/natalia_uva/ko1/fastq/KO1_S1_L001_R2_001.fastq.gz"),
                          ("/mnt/raid/zparteka/natalia_uva/ko2/fastq/KO2_S2_L001_R1_001.fastq.gz",
                           "/mnt/raid/zparteka/natalia_uva/ko2/fastq/KO2_S2_L001_R2_001.fastq.gz")]}

        for sam in samples.keys():
            sample = samples[sam]
            out_r1 = f"{sample[0][0].split('_')[0]}_{sample[0][0].split('_')[1]}_R1.fastq.gz"
            out_r2 = f"{sample[0][1].split('_')[0]}_{sample[0][0].split('_')[1]}_R2.fastq.gz"
            cat = local["cat"]
            (cat[sample[0][0], sample[1][0], ">", out_r1])()
            (cat[sample[0][1], sample[1][1], ">", out_r2])()

            sample.append((out_r1, out_r2))
            sample_luigi = luigi.DictParameter(sample)

            yield RunMapsPulledReplicates(sample_luigi)

            # todo take first replicate, build configuration (pass configuration in hichip_analysis)
            # todo run peak calling on first replicate

            # todo run MAPS on first replicate → need to save the location of feather output

            # todo take second replicate, build configuration

            # todo run peak calling on second replicate

            # todo run MAPS on second replicate → need to save the second location of feather output

            # todo pull replicates → need to pass names of the samples

            # todo build configuration on pulled samples

            # todo run hichip analysis on pulled replicates

            # todo run MAPS on pulled samples → needs results from MAPS run on separate replicates and peak calling on pulled samples







