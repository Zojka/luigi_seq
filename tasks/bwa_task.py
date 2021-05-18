#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from plumbum import local


class Mapping(luigi.Task):
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter()
    reference = luigi.Parameter()
    outname = luigi.Parameter(default="output.bam")

    def output(self):
        return luigi.LocalTarget(self.outname)

    def run(self):
        bwa = local["bwa"]
        (bwa["mem", "-SP5M", f"-t{self.threads}", self.reference, self.r1, self.r2] > self.outname)()
"""

class SamtoolsView(luigi.Task):

    def requires(self):
        return Mapping()

    def output(self):
        return luigi.LocalTarget(input())

    def run(self):
        samtools = local["samtools"]
        (samtools["view", "--bhS", self.input()] > "output.bam")()

"""
if __name__ == '__main__':
    luigi.run()
