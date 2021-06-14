#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from tasks.hichip_peak_calling import CallPeaks
from configuration.load_configuration import Configuration
from plumbum import local


class RunMapsSingleReplicate(luigi.Task):
    conf = luigi.DictParameter()

    def requires(self):
        return CallPeaks(conf)

    def output(self):
        return luigi.LocalTarget(self.peak_output)

    def run(self):
        # macs3
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", c.peak_quality, "-B", "-t", c.outnames["nodup"], "-n", self.peak_output])()


class PullReplicates(luigi.Task):
    peak_output = luigi.Parameter(c.outnames["peaks"])

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return luigi.LocalTarget(self.peak_output)

    def run(self):
        # macs3
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", c.peak_quality, "-B", "-t", c.outnames["nodup"], "-n", self.peak_output])()


class RunPeakCallingPulledReplicates(luigi.Task):
    pass


class RunMapsPulledReplicates(luigi.Task):
    sample = luigi.DictParameter()

    def requires(self):
        conf_s1 = Configuration(self.sample[0][0], self.sample[0][1])
        conf_s2 = Configuration(self.sample[1][0], self.sample[1][0])

        return RunMapsSingleReplicate(conf_s1), RunMapsSingleReplicate(conf_s2), RunPeakCallingPulledReplicates(
            self.sample)

    def output(self):
        return luigi.LocalTarget()

    def run(self):
        # macs3
        macs3 = local["sleep"]
        (macs3[5])()
