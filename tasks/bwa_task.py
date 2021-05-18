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
        samtools = local["samtools"]
        (bwa["mem", "-SP5M", f"-t{self.threads}", self.reference, self.r1, self.r2] | samtools[
            "view", "-bhS"] > self.outname)()


class RemoveNotAlignedReads(luigi.Task):
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter()
    reference = luigi.Parameter()
    outname = luigi.Parameter(default="output.bam")

    outname_mapped = luigi.Parameter(default="output_mapped.bam")

    def requires(self):
        return Mapping(r1=self.r1, r2=self.r2, threads=self.threads, reference=self.reference, outname=self.outname)

    def output(self):
        return luigi.LocalTarget(self.outname)

    def run(self):
        samtools = local["samtools"]
        (samtools["view", "-F", "0x04", "-b", self.outname] > self.outname_mapped)()

class MappingQualityFilter(luigi.Task):
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter()
    reference = luigi.Parameter()
    outname = luigi.Parameter(default="output.bam")

    outname_mapped = luigi.Parameter(default="output_mapped.bam")
    outname_filtered = "output_mapped_filtered.bam"
    quality = luigi.Parameter(default=30)

    def requires(self):
        return RemoveNotAlignedReads(r1=self.r1, r2=self.r2, threads=self.threads, reference=self.reference, outname=self.outname)

    def run(self):
        samtools = local["samtools"]
        (samtools["view", "-q", self.quality, "-t", self.threads, "-b", self.outname_mapped] > self.outname_filtered)()

if __name__ == '__main__':
    luigi.build()
