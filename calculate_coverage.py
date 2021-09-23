#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
import luigi
from plumbum import local
from tasks.configuration.load_configuration import samples
from tasks.hichip_peak_calling import CreateBigwig
from tasks.configuration.load_configuration import Configuration



class RunAnalysis(luigi.WrapperTask):

    def requires(self):

        for sam in samples.keys():
            sample = samples[sam]

            for sa in sample:
                conf= Configuration(sa[0], sa[1]).dumps()
                yield CreateBigwig(conf)
