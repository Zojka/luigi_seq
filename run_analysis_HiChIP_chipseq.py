#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import cmd
from tasks.configuration.chipseq_configuration import samples, chips, input
from tasks.maps_task import RunMapsPulledReplicates, RunMapsSingleReplicate, RunMapsWithChipSingleReplicate
from os.path import basename, dirname, join, isdir, isfile
from pathlib import Path
from os import makedirs

# todo implement hichip+chipseq analysis - start with single replicate
"""
requirements


run maps with chipseq → call peaks with input →
"""


# requirements
class RunAnalysis(luigi.WrapperTask):

    def requires(self):
        for sam in samples.keys():
            sample = samples[sam]
            chip = chips[sam]
            inp = input[sam]
            # samples = [hichip, chip, input]
            samp = [sample[0], chip[0], inp[0]]
            yield RunMapsWithChipSingleReplicate(samp)
