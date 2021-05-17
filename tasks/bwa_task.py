#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from os import system


class Mapping(luigi.Task):
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter()
    reference = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"{self.r1_file.split('R1')[0]}.bam")

    def run(self):
        command = f"bwa mem -SP5M -t{self.threads} {self.r1_file} {self.r2_file} > {self.output()}"
        system(command)


if __name__ == '__main__':
    luigi.run()
