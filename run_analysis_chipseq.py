#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""

# todo implement chip-seq data processing
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import local
from tasks.configuration.load_configuration import samples
from tasks.maps_task import RunMapsPulledReplicates, CalculateCoverage
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
                cat = local["cat"]
                (cat[sample[0][0], sample[1][0]] > out_r1)()
                (cat[sample[0][1], sample[1][1]] > out_r2)()

            if (out_r1, out_r2) not in sample:
                sample.append((out_r1, out_r2))
            print(sample)
            yield CalculateCoverage(sample)
