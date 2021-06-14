#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from plumbum import local
from tasks.configuration.load_configuration import Configuration

c = Configuration


class Mapping(luigi.Task):
    outname = luigi.Parameter(c.outnames["mapped"])

    def output(self):
        return luigi.LocalTarget(self.outname)

    def run(self):
        bwa = local["bwa"]
        samtools = local["samtools"]
        (bwa["mem", "-SP5M", f"-t{c.threads}", c.reference, c.r1, c.r2] | samtools[
            "view", "-bhS"] > self.outname)()


class RemoveNotAlignedReads(luigi.Task):
    outname_mapped = luigi.Parameter(c.outnames["mapped_only"])

    def requires(self):
        return Mapping()

    def output(self):
        return luigi.LocalTarget(self.outname_mapped)

    def run(self):
        samtools = local["samtools"]
        (samtools["view", "-F", "0x04", "-b", c.outnames["mapped"]] > self.outname_mapped)()


class MappingQualityFilter(luigi.Task):
    outname_filtered = luigi.Parameter(c.outnames["filtered"])

    def output(self):
        return luigi.LocalTarget(self.outname_filtered)

    def requires(self):
        return RemoveNotAlignedReads()

    def run(self):
        samtools = local["samtools"]
        (samtools["view", "-q", c.mapq, "-t", c.threads, "-b", c.outnames["mapped_only"]] > self.outname_filtered)()


class RemoveDuplicates(luigi.Task):
    outname_nodup = luigi.Parameter(c.outnames["nodup"])

    def requires(self):
        return MappingQualityFilter()

    def output(self):
        return luigi.LocalTarget(self.outname_nodup)

    def run(self):
        samtools = local["samtools"]
        (samtools["sort", "-n", "-t", c.threads, c.outnames["filtered"], "-o", "-"] | samtools[
            "fixmate", "--threads", c.threads, "-", "-"] | samtools["rmdup", "-S", "-", self.outname_nodup])()


class CreateBigwig(luigi.Task):
    """Create BigWig coverage file from deduplicated bam file. Needs samtools and deeptools"""

    outname_bigwig = luigi.Parameter(c.outnames["bigwig"])

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return luigi.LocalTarget(self.outname_bigwig)

    def run(self):
        samtools = local["samtools"]
        bamCoverage = local["bamCoverage"]
        (samtools["sort", "-t", c.threads, c.outnames["nodup"], "-o", c.outnames["sorted"]])()
        (samtools["index", c.outnames["sorted"]])()
        (bamCoverage["-b", c.outnames["sorted"], "-o", self.outname_bigwig])()


class CallPeaks(luigi.Task):
    config = luigi.DictParameter()
    peak_output = luigi.Parameter(c.outnames["peaks"])

    def requires(self):
        return RemoveDuplicates()

    def output(self):
        return luigi.LocalTarget(self.peak_output)

    def run(self):
        # macs3
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", self.config.peak_quality, "-B", "-t", self.config.outnames["nodup"], "-n", self.config.outnames["peaks"]])()


if __name__ == '__main__':
    luigi.build()
