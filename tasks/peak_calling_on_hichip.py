#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
# todo chip-seq configuration??

# todo change to hichip specific mapping according to 4dn recommendations

import luigi
from plumbum import local, cmd, FG
# from tasks.configuration.load_configuration import Configuration, loads
from tasks.configuration.hichip_configuration import Configuration
from tasks.configuration.hichip_configuration import loads


class Mapping(luigi.Task):
    c = luigi.DictParameter()

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["mapped"])

    def run(self):
        config = loads(self.c)
        with local.env(THREADS=config.threads, REFERENCE=config.reference, R1=config.r1, R2=config.r2,
                       OUTNAME_MAPPED=config.outnames["mapped"]):
            run_bwa = local["./tasks/map_hichip.sh"]
            (run_bwa())


class MappingQualityFilter(luigi.Task):
    c = luigi.DictParameter()

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["filtered"])

    def requires(self):
        return Mapping(self.c)

    def run(self):
        config = loads(self.c)
        samtools = local["samtools"]
        (samtools[
             "view", "-q", config.mapq, "-t", config.threads, "-b", config.outnames["mapped"]] >
         config.outnames["filtered"])()


class ParsePairtools(luigi.Task):
    c = luigi.DictParameter()

    def requires(self):
        return Mapping(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["paired"])

    def run(self):
        config = loads(self.c)

        with local.env(THREADS=config.threads, CHROMOSOMES=config.chromosomes, MEM=config.memory,
                       SORTED_PAIRS_PATH=config.outnames["paired"], TMPDIR=config.outdir,
                       MAPPED=config.outnames["mapped"]):
            run_pairtools = local["./tasks/run_pairtools.sh"]
            (run_pairtools())


class RemoveDuplicates(luigi.Task):
    c = luigi.DictParameter()

    def requires(self):
        return ParsePairtools(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["nodup"])

    def run(self):
        config = loads(self.c)
        pairtools = local["pairtools"]
        (pairtools[
            "dedup","--mark-dups", "--output-stats", config.outnames["dup_stats"], "--output", config.outnames["nodup"],
            config.outnames["paired"]])()


class Pairsam2Bam(luigi.Task):
    c = luigi.DictParameter()

    def requires(self):
        return RemoveDuplicates(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["final_bam"])

    def run(self):
        config = loads(self.c)
        pairtools = local["pairtools"]
        (pairtools["split", "--output-sam", config.outnames["final_bam"], config.outnames["nodup"]])()


class CreateBigwig(luigi.Task):
    """Create BigWig coverage file from deduplicated bam file. Needs samtools and deeptools"""
    c = luigi.DictParameter()

    def requires(self):
        return Pairsam2Bam(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["bigwig"])

    def run(self):
        config = loads(self.c)
        samtools = local["samtools"]
        bamCoverage = local["bamCoverage"]
        (samtools[
            "sort", "-t", 4, "-m", "16G", config.outnames["final_bam"], "-o", config.outnames["sorted"]])()
        (samtools["index", config.outnames["sorted"]])()
        (bamCoverage["-b", config.outnames["sorted"], "-o", config.outnames["bigwig"]])()


class CallPeaks(luigi.Task):
    c = luigi.DictParameter()

    def requires(self):
        return CreateBigwig(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["peaks"] + "_peaks.narrowPeak")

    def run(self):
        # macs3
        config = loads(self.c)
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", config.peak_quality, "-B", "-t", config.outnames["final_bam"], "-n",
            config.outnames["peaks"], "-g", config.genome_size, "-f", "BAMPE"])()


if __name__ == '__main__':
    luigi.build()
