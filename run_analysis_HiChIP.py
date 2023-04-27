#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import cmd
from tasks.configuration.cohesin_project_data import samples
from tasks.maps_task import RunMapsPulledReplicates, RunMapsSingleReplicate, RunMapsMultipleReplicates
from tasks.configuration.load_configuration import Configuration
from os.path import dirname, join, isdir, isfile
from pathlib import Path
from os import makedirs


class RunAnalysis(luigi.WrapperTask):

    def requires(self):

        for sam in samples.keys():
            sample = samples[sam]
            if len(sample) > 1:
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

                print(sample)
                # temporary modification
                yield RunMapsPulledReplicates(sample)
            else:
                print("single replicate")
                conf = Configuration(sample[0][0], sample[0][1]).dumps()
                yield RunMapsSingleReplicate(conf)

