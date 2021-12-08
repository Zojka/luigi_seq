#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import cmd
from tasks.configuration.chipseq_configuration import samples
from tasks.maps_task import RunMapsPulledReplicates
from os.path import basename, dirname, join, isdir, isfile
from pathlib import Path
from os import makedirs



class RunAnalysis(luigi.WrapperTask):

    def requires(self):

        for sam in samples.keys():
            sample = samples[sam]
            folder = join(Path(dirname(sample[0][0])).parent.parent.absolute(), f"{sam}_pulled/fastq/")
            if not isdir(folder):
                makedirs(folder)
            out_r1 = join(folder, f"{sam}_pulled_R1.fastq.gz")
            out_r2 = join(folder, f"{sam}_pulled_R2.fastq.gz")

            if isfile(out_r2) and isfile(out_r1):
                pass
            else:
                r1 = [s[0] for s in sample]
                r2 = [s[1] for s in sample]
                (cmd.cat.__getitem__(r1) > out_r1)()
                (cmd.cat.__getitem__(r2) > out_r2)()
            if (out_r1, out_r2) not in sample:
                sample.append((out_r1, out_r2))

            if (out_r1, out_r2) not in sample:
                sample.append((out_r1, out_r2))
            print(sample)
            yield RunMapsPulledReplicates(sample)
