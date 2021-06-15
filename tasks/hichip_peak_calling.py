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
    c = luigi.DictParameter()

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["mapped"])

    def run(self):
        config = loads(self.c)

        # bwa = local["bwa"]
        # samtools = local["samtools"]
        # todo -v for debugging
        # (bwa["mem", "-SP5M", "-v", 0, f"-t{config.threads}", config.reference, config.r1, config.r2] |
        #  samtools["view", "-bhS", "-"] > config.outnames["mapped"])()

        # bwa mem -SP5M -v 0 -t${THREADS} ${REFERENCE} ${R1} ${R2} | samtools view -bhS - > ${OUTNAME_MAPPED}
        with local.env(THREADS=config.threads, REFERENCE=config.reference, R1=config.r1, R2=config.r2,
                       OUTNAME_MAPPED=config.outnames["mapped"]):
            run_bwa = local["./tasks/map.sh"]
            (run_bwa())


class RemoveNotAlignedReads(luigi.Task):
    c = luigi.DictParameter()

    def requires(self):
        return Mapping(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["mapped_only"])

    def run(self):
        config = loads(self.c)

        samtools = local["samtools"]
        (samtools["view", "-F", "0x04", "-b", config.outnames["mapped"]] > config.outnames["mapped_only"])()


class MappingQualityFilter(luigi.Task):
    c = luigi.DictParameter()

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["filtered"])

    def requires(self):
        return RemoveNotAlignedReads(self.c)

    def run(self):
        config = loads(self.c)
        samtools = local["samtools"]
        (samtools[
             "view", "-q", config.mapq, "-t", config.threads, "-b", config.outnames["mapped_only"]] >
         config.outnames["filtered"])()


class RemoveDuplicates(luigi.Task):
    c = luigi.DictParameter()

    def requires(self):
        return MappingQualityFilter(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["nodup"])

    def run(self):
        print("samtools")

        config = loads(self.c)
        samtools = local["samtools"]
        (samtools["sort", "-n", "-t", config.threads, config.outnames["filtered"], "-o", "-"] | samtools[
            "fixmate", "--threads", config.threads, "-", "-"] | samtools[
             "rmdup", "-S", "-", config.outnames["nodup"]])()


class CreateBigwig(luigi.Task):
    """Create BigWig coverage file from deduplicated bam file. Needs samtools and deeptools"""
    c = luigi.DictParameter()

    def requires(self):
        return RemoveDuplicates(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["bigwig"])

    def run(self):
        config = loads(self.c)
        samtools = local["samtools"]
        bamCoverage = local["bamCoverage"]
        (samtools[
            "sort", "-t", config.threads, config.outnames["nodup"], "-o", config.outnames["sorted"]])()
        (samtools["index", config.outnames["sorted"]])()
        (bamCoverage["-b", config.outnames["sorted"], "-o", config.outnames["bigwig"]])()


class CallPeaks(luigi.Task):
    c = luigi.DictParameter()

    def requires(self):
        return RemoveDuplicates(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["peaks"] + "_peaks.narrowPeak")

    def run(self):
        print("callpeaks")

        # macs3
        config = loads(self.c)
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", config.peak_quality, "-B", "-t", config.outnames["nodup"], "-n",
            config.outnames["peaks"]])()


if __name__ == '__main__':
    luigi.build()
