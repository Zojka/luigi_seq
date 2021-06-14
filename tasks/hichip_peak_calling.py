#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from plumbum import local
from tasks.configuration.load_configuration import Configuration, loads

c = Configuration


class Mapping(luigi.Task):
    config = loads(luigi.Parameter())

    def output(self):
        return luigi.LocalTarget(self.config.outnames["mapped"])

    def run(self):
        bwa = local["bwa"]
        samtools = local["samtools"]
        (bwa["mem", "-SP5M", f"-t{self.config.threads}", self.config.reference, self.config.r1, self.config.r2] |
         samtools["view", "-bhS"] > self.config.outnames["mapped"])()


class RemoveNotAlignedReads(luigi.Task):
    config = loads(luigi.Parameter())

    def requires(self):
        return Mapping(self.config)

    def output(self):
        return luigi.LocalTarget(self.config.outnames["mapped_only"])

    def run(self):
        samtools = local["samtools"]
        (samtools["view", "-F", "0x04", "-b", self.config.outnames["mapped"]] > self.config.outnames["mapped_only"])()


class MappingQualityFilter(luigi.Task):
    config = loads(luigi.Parameter())

    def output(self):
        return luigi.LocalTarget(self.config.outnames["filtered"])

    def requires(self):
        return RemoveNotAlignedReads(self.config)

    def run(self):
        samtools = local["samtools"]
        (samtools[
             "view", "-q", self.config.mapq, "-t", self.config.threads, "-b", self.config.outnames["mapped_only"]] >
         self.config.outnames["filtered"])()


class RemoveDuplicates(luigi.Task):
    config = loads(luigi.Parameter())

    # outname_nodup = luigi.Parameter(self.config.outnames["nodup"])

    def requires(self):
        return MappingQualityFilter(self.config)

    def output(self):
        return luigi.LocalTarget(self.config.outnames["nodup"])

    def run(self):
        samtools = local["samtools"]
        (samtools["sort", "-n", "-t", self.config.threads, self.config.outnames["filtered"], "-o", "-"] | samtools[
            "fixmate", "--threads", self.config.threads, "-", "-"] | samtools[
             "rmdup", "-S", "-", self.config.outnames["nodup"]])()


class CreateBigwig(luigi.Task):
    """Create BigWig coverage file from deduplicated bam file. Needs samtools and deeptools"""
    config = loads(luigi.Parameter())

    # outname_bigwig = luigi.Parameter(self.config.outnames["bigwig"])

    def requires(self):
        return RemoveDuplicates(self.config)

    def output(self):
        return luigi.LocalTarget(self.config.outnames["bigwig"])

    def run(self):
        samtools = local["samtools"]
        bamCoverage = local["bamCoverage"]
        (samtools[
            "sort", "-t", self.config.threads, self.config.outnames["nodup"], "-o", self.config.outnames["sorted"]])()
        (samtools["index", self.config.outnames["sorted"]])()
        (bamCoverage["-b", self.config.outnames["sorted"], "-o", self.config.outnames["bigwig"]])()


class CallPeaks(luigi.Task):
    config = loads(luigi.Parameter())

    # peak_output = luigi.Parameter(self.config.outnames["peaks"])

    def requires(self):
        return RemoveDuplicates(self.config)

    def output(self):
        return luigi.LocalTarget(self.config.outnames["peaks"])

    def run(self):
        # macs3
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", self.config.peak_quality, "-B", "-t", self.config.outnames["nodup"], "-n",
            self.config.outnames["peaks"]])()


if __name__ == '__main__':
    luigi.build()
