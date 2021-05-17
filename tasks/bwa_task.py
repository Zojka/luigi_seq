#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from os import system
from luigi.contrib.external_program import ExternalProgramTask, ExternalPythonProgramTask
from luigi.contrib.external_program import ExternalProgramRunError


class Mapping(ExternalProgramTask):
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter()
    reference = luigi.Parameter()

    def program_args(self):
        return ["bwa", "mem"] #, "-SP5M", f"-t{self.threads}", self.reference, self.r1, self.r2]

    def output(self):
        return luigi.LocalTarget("output.bam")


if __name__ == '__main__':
    luigi.run()
