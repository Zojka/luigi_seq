#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from tasks.hichip_peak_calling import CallPeaks
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
