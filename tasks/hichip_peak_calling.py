#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from plumbum import local
from configuration.load_configuration import load

c = load()


class Mapping(luigi.Task):
    r1 = luigi.Parameter()
    r2 = luigi.Parameter()
    threads = luigi.Parameter(c.threads)
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
    outname_sorted = luigi.Parameter(default="output_sorted.bam")

    def requires(self):
        return RemoveDuplicates(r1=self.r1, r2=self.r2, threads=self.threads, reference=self.reference,
                                outname=self.outname, quality=self.quality, outname_mapped=self.outname_mapped,
                                outname_filtered=self.outname_filtered, outname_nodup=self.outname_nodup)

    def output(self):
        return luigi.LocalTarget(self.outname_bigwig)

    def run(self):
        samtools = local["samtools"]
        bamCoverage = local["bamCoverage"]
        (samtools["sort", "-t", self.threads, self.outname_nodup, "-o", self.outname_sorted])()
        (samtools["index", self.outname_sorted])()
        (bamCoverage["-b", self.outname_sorted, "-o", self.outname_bigwig])()


class CallPeaks(luigi.Task):
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
    outname_sorted = luigi.Parameter(default="output_sorted.bam")
    peak_quality = luigi.Parameter(default=0.01)
    peak_output = luigi.Parameter(default="peaks_macs3")

    def requires(self):
        return RemoveDuplicates(r1=self.r1, r2=self.r2, threads=self.threads, reference=self.reference,
                                outname=self.outname, quality=self.quality, outname_mapped=self.outname_mapped,
                                outname_filtered=self.outname_filtered, outname_nodup=self.outname_nodup)

    def output(self):
        return luigi.LocalTarget(self.peak_output)

    def run(self):
        # macs3
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", self.peak_quality, "-B", "-t", self.outname_nodup, "-n", self.peak_output])()


if __name__ == '__main__':
    luigi.build()
