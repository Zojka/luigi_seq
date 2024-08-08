#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from tasks.peak_calling import CallPeaks, CallPeaksWithInput
from tasks.configuration.chipseq_configuration import Configuration as ChipConfiguration
from tasks.configuration.load_configuration import Configuration, loads
from plumbum import local


class RunMapsSingleReplicate(luigi.Task):
    c = luigi.DictParameter()

    def requires(self):
        return CallPeaks(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["maps"])

    def run(self):
        config = loads(self.c)
        with local.env(DATASET_NUMBER=1, DATASET_NAME=config.maps_dataset, FASTQDIR=config.fastq_dir,
                       OUTDIR=config.outdir, MACS_OUTPUT=config.narrow_peak, BWA_INDEX=config.bwa_index,
                       MAPQ=config.mapq, THREADS=config.threads):
            run_maps = local["./tasks/run_maps.sh"]
            (run_maps >> "maps.txt")()

# todo this only works for two replicates!!!
class RunMapsPulledReplicates(luigi.Task):
    sample = luigi.DictParameter()

    def requires(self):
        conf_s1 = Configuration(self.sample[0][0], self.sample[0][1]).dumps()
        conf_s2 = Configuration(self.sample[1][0], self.sample[1][1]).dumps()
        conf_s3 = Configuration(self.sample[2][0], self.sample[2][1]).dumps()

        return RunMapsSingleReplicate(conf_s1), RunMapsSingleReplicate(conf_s2), CallPeaks(conf_s3)

    def output(self):
        config = Configuration(self.sample[2][0], self.sample[2][1])
        return luigi.LocalTarget(config.outnames["maps"])

    def run(self):
        conf_s1 = Configuration(self.sample[0][0], self.sample[0][1])
        conf_s2 = Configuration(self.sample[1][0], self.sample[1][1])
        conf_s3 = Configuration(self.sample[2][0], self.sample[2][1])

        feather1 = f"{conf_s1.outdir}/feather_output/{conf_s1.maps_dataset}_current/"
        feather2 = f"{conf_s2.outdir}/feather_output/{conf_s2.maps_dataset}_current/"
        print(feather1, feather2)

        with local.env(DATASET_NUMBER=2, DATASET_NAME=conf_s3.maps_dataset, FASTQDIR="",
                       OUTDIR=conf_s3.outdir, MACS_OUTPUT=conf_s3.narrow_peak, BWA_INDEX=conf_s3.bwa_index,
                       MAPQ=conf_s3.mapq, THREADS=conf_s3.threads, DATASET1=feather1, DATASET2=feather2):
            run_maps = local["./tasks/run_maps.sh"]
            (run_maps > "done.txt")()

class RunMapsMultipleReplicates(luigi.Task):
    # todo fixed number of replicates?
    sample = luigi.DictParameter()

    def requires(self):
        conf_s1 = Configuration(self.sample[0][0], self.sample[0][1]).dumps()
        conf_s2 = Configuration(self.sample[1][0], self.sample[1][1]).dumps()
        conf_s3 = Configuration(self.sample[2][0], self.sample[2][1]).dumps()
        conf_s4 = Configuration(self.sample[3][0], self.sample[3][1]).dumps()

        return RunMapsSingleReplicate(conf_s1), RunMapsSingleReplicate(conf_s2), RunMapsSingleReplicate(conf_s3), CallPeaks(conf_s4)

    def output(self):
        config = Configuration(self.sample[3][0], self.sample[3][1])
        return luigi.LocalTarget(config.outnames["maps"])

    def run(self):
        conf_s1 = Configuration(self.sample[0][0], self.sample[0][1])
        conf_s2 = Configuration(self.sample[1][0], self.sample[1][1])
        conf_s3 = Configuration(self.sample[2][0], self.sample[2][1])
        conf_s4 = Configuration(self.sample[3][0], self.sample[3][1])

        feather1 = f"{conf_s1.outdir}/feather_output/{conf_s1.maps_dataset}_current/"
        feather2 = f"{conf_s2.outdir}/feather_output/{conf_s2.maps_dataset}_current/"
        feather3 = f"{conf_s3.outdir}/feather_output/{conf_s3.maps_dataset}_current/"
        print(feather1, feather2, feather3)

        with local.env(DATASET_NUMBER=3, DATASET_NAME=conf_s4.maps_dataset, FASTQDIR="",
                       OUTDIR=conf_s4.outdir, MACS_OUTPUT=conf_s4.narrow_peak, BWA_INDEX=conf_s4.bwa_index,
                       MAPQ=conf_s4.mapq, THREADS=conf_s4.threads, DATASET1=feather1, DATASET2=feather2, DATASET3=feather3):
            run_maps = local["./tasks/run_maps.sh"]
            (run_maps > "done.txt")()

class RunMapsWithChipSingleReplicate(luigi.Task):
    # samples = [hichip, chip, input]
    sample = luigi.Parameter()

    def requires(self):
        sam = [[self.sample[1][0], self.sample[1][1]], [self.sample[2][0], self.sample[2][1]]]
        # sam = [[data_R1, data_R2], [input_R1, input_R2]]
        return CallPeaksWithInput(sam)

    def output(self):
        config = Configuration(self.sample[0][0], self.sample[0][1])
        return luigi.LocalTarget(config.outnames["maps"])

    def run(self):
        chip_conf = ChipConfiguration(self.sample[1][0], self.sample[1][1])
        hichip_conf = Configuration(self.sample[0][0], self.sample[0][1])
        with local.env(DATASET_NUMBER=1, DATASET_NAME=hichip_conf.maps_dataset, FASTQDIR=hichip_conf.fastq_dir,
                       OUTDIR=hichip_conf.outdir, MACS_OUTPUT=chip_conf.narrow_peak, BWA_INDEX=hichip_conf.bwa_index,
                       MAPQ=hichip_conf.mapq, THREADS=hichip_conf.threads):
            run_maps = local["./tasks/run_maps.sh"]
            (run_maps >> "maps.txt")()
