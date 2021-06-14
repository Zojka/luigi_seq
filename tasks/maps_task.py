#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from tasks.hichip_peak_calling import CallPeaks
from tasks.configuration.load_configuration import Configuration
from plumbum import local


class RunMapsSingleReplicate(luigi.Task):
    conf = luigi.DictParameter()

    def requires(self):
        return CallPeaks(self.conf)

    def output(self):
        return luigi.LocalTarget()

    def run(self):
        # macs3
        macs3 = local["sleep"]
        (macs3[5])()


class RunMapsPulledReplicates(luigi.Task):
    sample = luigi.DictParameter()

    def requires(self):
        conf_s1 = Configuration(self.sample[0][0], self.sample[0][1])
        conf_s2 = Configuration(self.sample[1][0], self.sample[1][1])
        conf_s3 = Configuration(self.sample[2][0], self.sample[2][1])

        return RunMapsSingleReplicate(conf_s1), RunMapsSingleReplicate(conf_s2), CallPeaks(conf_s3)

    # def output(self):
    #     return luigi.LocalTarget()

    def run(self):
        # macs3
        macs3 = local["sleep"]
        (macs3[5])()
