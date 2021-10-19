#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

# todo implement chip-seq data processing
# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import local, FG, cmd
from tasks.configuration.chipseq_configuration import chips, input, Configuration
from tasks.peak_calling import RunPeakCallingOnReplicates
from os.path import basename, dirname, join, isdir, isfile
from pathlib import Path
from os import makedirs


# todo add information about input - needs to be trated as "normal" chip-seq file

class RunAnalysis(luigi.WrapperTask):

    def requires(self):

        for sam in chips.keys():
            # sample = [[(rep1_R1, rep1_R2), (rep2_R1, rep2_R2)], [(inp1_R1, inp2_R2),

            # chip-seq samples - pulling replicates
            sample = chips[sam]
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
                cat = local["cat"]
                print(r1_str)
                print(r2_str)

                # (cat[r1_str] > out_r1)()
                # (cat[r2_str] > out_r2)()
                # r1 += ">"
                # r1 += out_r1
                # cmd.cat.__getitem__(r1) & FG
                # r2 += ">"
                # r2 += out_r2
                # cmd.cat.__getitem__(r1) & FG
                (cmd.cat.__getitem__(r1) & FG > out_r1)()
                # r2 += ">"
                # r2 += out_input_r2
                (cmd.cat.__getitem__(r2) & FG > out_r2)()
            if (out_r1, out_r2) not in sample:
                sample.append((out_r1, out_r2))
            print(sample)

            # control data (input) - pulling replicates (if needed)
            inp = input[sam]
            if isinstance(inp, list):
                folder = join(Path(dirname(inp[0][0])).parent.parent.absolute(), f"control_pulled/fastq/")
                if not isdir(folder):
                    makedirs(folder)
                out_input_r1 = join(folder, f"{sam}_igg_pulled_R1.fastq.gz")
                out_input_r2 = join(folder, f"{sam}_igg_pulled_R2.fastq.gz")

                if isfile(out_input_r1) and isfile(out_input_r2):
                    pass
                else:
                    r1_inp = [s[0] for s in inp]
                    r2_inp = [s[1] for s in inp]
                    r1_inp_str = ' '.join(r1_inp)
                    r2_inp_str = ' '.join(r2_inp)
                    print(r1_inp_str)
                    print(r2_inp_str)
                    cat = local["cat"]

                    # (cat[r1_inp_str] > out_input_r1)()
                    # (cat[r2_inp_str] > out_input_r2)()
                    # r1 += ">"
                    # r1 += out_input_r1
                    (cmd.cat.__getitem__(r1_inp) & FG > out_input_r1)()
                    # r2 += ">"
                    # r2 += out_input_r2
                    (cmd.cat.__getitem__(r2_inp) & FG > out_input_r2)()

                if (out_input_r1, out_input_r2) not in sample:
                    inp.append((out_input_r1, out_input_r2))
                break
            # yield RunPeakCallingOnReplicates(sample)
