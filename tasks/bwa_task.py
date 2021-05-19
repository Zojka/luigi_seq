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
        return luigi.LocalTarget(self.outname_mapped)

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
    outname_filtered = luigi.Parameter(default="output_mapped_filtered.bam")
    quality = luigi.Parameter(default=30)

    def output(self):
        return luigi.LocalTarget(self.outname_filtered)

    def requires(self):
        return RemoveNotAlignedReads(r1=self.r1, r2=self.r2, threads=self.threads, reference=self.reference,
                                     outname=self.outname, outname_mapped=self.outname_mapped)

    def run(self):
        samtools = local["samtools"]
        (samtools["view", "-q", self.quality, "-t", self.threads, "-b", self.outname_mapped] > self.outname_filtered)()


class RemoveDuplicates(luigi.Task):
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter()
    reference = luigi.Parameter()
    outname = luigi.Parameter(default="output.bam")
    outname_mapped = luigi.Parameter(default="output_mapped.bam")
    outname_filtered = luigi.Parameter(default="output_mapped_filtered.bam")
    outname_nodup = luigi.Parameter(default="output_mapped_filtered_nodup.bam")
    quality = luigi.Parameter(default=30)

    def requires(self):
        return MappingQualityFilter(r1=self.r1, r2=self.r2, threads=self.threads, reference=self.reference,
                                    outname=self.outname, quality=self.quality, outname_mapped=self.outname_mapped,
                                    outname_filtered=self.outname_filtered)

    def output(self):
        return luigi.LocalTarget(self.outname_nodup)

    def run(self):
        samtools = local["samtools"]
        (samtools["sort", "-n", "-t", self.threads, self.outname_filtered, "-o", "-"] | samtools[
            "fixmate", "--threads", self.threads, "-", "-"] | samtools["rmdup", "-S", "-", self.outname_nodup])()


class CreateBigwig(luigi.Task):
    """Create BigWig coverage file from deduplicated bam file. Needs samtools and deeptools"""
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter()
    reference = luigi.Parameter()
    outname = luigi.Parameter(default="output.bam")
    outname_mapped = luigi.Parameter(default="output_mapped.bam")
    outname_filtered = luigi.Parameter(default="output_mapped_filtered.bam")
    outname_nodup = luigi.Parameter(default="output_mapped_filtered_nodup.bam")
    quality = luigi.Parameter(default=30)
    outname_bigwig = luigi.Parameter(default="output.bw")
    outname_index = luigi.Parameter(default="output_indexed.bam")

    def requires(self):
        return RemoveDuplicates(r1=self.r1, r2=self.r2, threads=self.threads, reference=self.reference,
                                outname=self.outname, quality=self.quality, outname_mapped=self.outname_mapped,
                                outname_filtered=self.outname_filtered, outname_nodup=self.outname_nodup)

    def output(self):
        return luigi.LocalTarget(self.outname_bigwig)

    def run(self):
        # todo this is not poducing an output file
        samtools = local["samtools"]
        bamCoverage = local["bamCoverage"]
        (samtools["sort", self.outname_nodup, "-o", self.outname_index])()
        (samtools["index", self.outname_index] > self.outname_index)()
        (bamCoverage["-b", self.outname_index, "-o", self.outname_bigwig])()


if __name__ == '__main__':
    luigi.build()
