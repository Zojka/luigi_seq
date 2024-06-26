#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

# todo chip-seq configuration??
# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import cmd
from tasks.configuration.chipseq_configuration import chips, input
from tasks.peak_calling import RunPeakCallingOnReplicates
from os.path import dirname, join, isdir, isfile
from pathlib import Path
from os import makedirs, path


# todo add information about input - needs to be trated as "normal" chip-seq file

class RunAnalysis(luigi.WrapperTask):

    def requires(self):
        task_list = []
        for sam in chips.keys():

            # chip-seq samples - pulling replicates
            sample = chips[sam]
            print(sample)
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
                    r1_str = ' '.join(r1)
                    r2_str = ' '.join(r2)
                    print(r1_str)
                    print(r2_str)
                    (cmd.cat.__getitem__(r1) > out_r1)()
                    (cmd.cat.__getitem__(r2) > out_r2)()
                if (out_r1, out_r2) not in sample:
                    sample.append((out_r1, out_r2))
                print(sample)

            # control data (input) - pulling replicates (if needed)
            # todo - input without replicates
            inp = input[sam]
            print("here", inp)
            if len(inp) > 1:
                if len(inp) != len(sample):
                    raise Exception("If you want to pull the input replicates you have to have an input for all samples.")
                name = path.basename(Path(dirname(inp[0][0])).parent.parent)
                folder = join(Path(dirname(inp[0][0])).parent.parent.absolute(), f"{name}_pulled/fastq/")
                if not isdir(folder):
                    makedirs(folder)
                out_input_r1 = join(folder, f"{name}_igg_pulled_R1.fastq.gz")
                out_input_r2 = join(folder, f"{name}_igg_pulled_R2.fastq.gz")

                if isfile(out_input_r1) and isfile(out_input_r2):
                    pass
                else:
                    # todo test pooling replicates
                    r1_inp = [s[0] for s in inp]
                    r2_inp = [s[1] for s in inp]
                    r1_inp_str = ' '.join(r1_inp)
                    r2_inp_str = ' '.join(r2_inp)
                    print(r1_inp_str)
                    print(r2_inp_str)
                    (cmd.cat.__getitem__(r1_inp) > out_input_r1)()
                    (cmd.cat.__getitem__(r2_inp) > out_input_r2)()
                if (out_input_r1, out_input_r2) not in inp:
                    inp.append((out_input_r1, out_input_r2))
            else:
                # todo does it work?
                # use the same input file for all samples
                while len(inp) < len(sample):
                    inp.append(inp[0])
                    print(inp)

            task_list.append(RunPeakCallingOnReplicates([sample, inp]))
        return task_list