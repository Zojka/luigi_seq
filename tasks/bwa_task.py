#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
import subprocess
from luigi.contrib.external_program import ExternalProgramTask, ExternalPythonProgramTask
from luigi.contrib.external_program import ExternalProgramRunError


class Mapping(luigi.Task):
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter()
    reference = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("output.bam")

    def run(self):
        subprocess.run(
            ["bwa", "mem", "-SP5M", f"-t{self.threads}", self.reference, self.r1, self.r2, ">", self.output()])


if __name__ == '__main__':
    luigi.run()
