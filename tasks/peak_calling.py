#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

import luigi
from plumbum import local, cmd, FG
from tasks.configuration.load_configuration import Configuration, loads
from os.path import join, dirname, isdir
from os import makedirs
from pathlib import Path

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
        return RemoveDuplicates(self.c), CreateBigwig(self.c)

    def output(self):
        config = loads(self.c)
        return luigi.LocalTarget(config.outnames["peaks"] + "_peaks.narrowPeak")

    def run(self):
        # macs3
        config = loads(self.c)
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", config.peak_quality, "-B", "-t", config.outnames["nodup"], "-n",
            config.outnames["peaks"]])()


# todo test this
class CallPeaksWithInput(luigi.Task):
    # sample = [[data_R1, data_R2], [input_R1, input_R2]]
    sample = luigi.Parameter()
    conf_sample = Configuration(sample[0][0], sample[0][1]).dumps()
    conf_input = Configuration(sample[1][0], sample[1][1]).dumps()

    def requires(self):
        list_of_tasks = [CreateBigwig(self.conf_sample), CreateBigwig(self.conf_input)]
        return list_of_tasks

    def output(self):
        return luigi.LocalTarget(self.conf_sample.outnames["peaks"] + "_peaks.narrowPeak")

    def run(self):
        # macs3
        macs3 = local["macs3"]
        (macs3[
            "callpeak", "--nomodel", "-q", self.conf_sample.peak_quality, "-B", "-t", self.conf_sample.outnames[
                "nodup"],
            "-c", self.conf_input.outnames["nodup"], "-n", self.conf_sample.outnames["peaks"]])()


# todo wraperr task - test this
class RunPeakCallingOnReplicates(luigi.WrapperTask):
    # samples = [[replicates], [inputs]]
    samples = luigi.Parameter()
    def requires(self):
        for i in range(len(self.samples[0])):
            if isinstance(self.samples[1], list):
                sample = [self.samples[0][i], self.samples[1][i]]
            else:
                sample = [self.samples[0][i], self.samples[1]]
            yield CallPeaksWithInput(sample)



# todo implement this
class RunPeakCallingOnPulledReplicates(luigi.Task):
    # samples = [[replicates], [inputs]]
    # todo run pulling replicates
    # todo run pulling input if inputs are on a list
    # todo run callpeaks with input

    samples = luigi.Parameter()


class ReplicatePulling(luigi.Task):
    # samples = [rep1, rep2, ....]
    #

    samples = luigi.Parameter()
    conf_sample = Configuration(samples[0][0], samples[0][1]).dumps()
    r1 = [sam[0] for sam in samples]
    r2 = [sam[1] for sam in samples]

    # todo change this to something better
    folder = join(Path(dirname(r1[0])).parent.parent.absolute(), f"{r1[0][:-11]}_pulled/fastq/")
    if not isdir(folder):
        makedirs(folder)
    out_r1 = join(folder, f"{r1[0][:-11]}_pulled_R1.fastq.gz")
    out_r2 = join(folder, f"{r1[0][:-11]}_pulled_R2.fastq.gz")

    def output(self):
        return luigi.LocalTarget(self.out_r1, self.out_r2)

    def run(self):
        cat = local["cat"]

        self.r1 += ">"
        self.r1 += self.out_r1
        (cat.__getitem__(self.r1))
        self.r2 += ">"
        self.r2 += self.out_r2
        (cat.__getitem__(self.r2))


# todo

class CheckSimilatity(luigi.Task):
    # implement usage of HPrep - probably not here
    pass

if __name__ == '__main__':
    luigi.build()
