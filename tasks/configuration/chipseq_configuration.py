#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: zparteka
"""
from os import makedirs
from os.path import basename, join, isdir, dirname

# chipseq analysis configuration - if using input - each sample has to have matching input in input dict

"""genome sizes
hs: 2.7e9
mm: 1.87e9
ce: 9e7
dm: 1.2e8

"""
chips_yoruban = {"gm19240_ctcf": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/chip_seq/chip_ctcf_i/fastq/SRR998409_R1.fastq.gz",
                                   "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/chip_seq/chip_ctcf_i/fastq/SRR998409_R2.fastq.gz"),
                                    ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/chip_seq/chip_ctcf_ii/fastq/SRR998410_R1.fastq.gz",
                                   "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/chip_seq/chip_ctcf_ii/fastq/SRR998410_R2.fastq.gz")]}

input_yoruban = {"gm19240_ctcf": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/chip_seq//chip_input/fastq/SRR998427_R1.fastq.gz",
                                   "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/chip_seq//chip_input/fastq/SRR998427_R2.fastq.gz")]}



samples_puerto = {"hg00731_smc1": [("/mnt/raid/zparteka/hichip/puerto_rican/HG00731/smc1/raw_data/rep1/fastq/Rep1_Smc1_HiChiP_HG00731_R1.fastq.gz",
                                    "/mnt/raid/zparteka/hichip/puerto_rican/HG00731/smc1/raw_data/rep1/fastq/Rep1_Smc1_HiChiP_HG00731_R2.fastq.gz"),
                                   ("/mnt/raid/zparteka/hichip/puerto_rican/HG00731/smc1/raw_data/rep2/fastq/Rep2_Smc1_HiChiP_HG00731_R1.fastq.gz",
                                    "/mnt/raid/zparteka/hichip/puerto_rican/HG00731/smc1/raw_data/rep2/fastq/Rep2_Smc1_HiChiP_HG00731_R2.fastq.gz")]}


chips_uva = {"wt_k9ac": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_K9AC/WT1_K9AC/fastq/wt1_k9ac_a2_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_K9AC/WT1_K9AC/fastq/wt1_k9ac_a2_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_K9AC/WT2_K9AC/fastq/wt2_k9ac_c2_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_K9AC/WT2_K9AC/fastq/wt2_k9ac_c2_R2.fastq.gz")],

         "ko_k9ac": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_K9AC/KO1_K9AC/fastq/ko1_k9ac_b2_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_K9AC/KO1_K9AC/fastq/ko1_k9ac_b2_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_K9AC/KO2_K9AC/fastq/ko2_k9ac_d2_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_K9AC/KO2_K9AC/fastq/ko2_k9ac_d2_R2.fastq.gz")],

         "wt_sirt6": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_SIRT6/WT1_SIRT6/fastq/wt1_sirt6_a1_R1.fq.gz",
                       "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_SIRT6/WT1_SIRT6/fastq/wt1_sirt6_a1_R2.fq.gz"),
                      ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_SIRT6/WT2_SIRT6/fastq/wt2_sirt6_c1_R1.fastq.gz",
                       "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_SIRT6/WT2_SIRT6/fastq/wt2_sirt6_c1_R2.fastq.gz")],

         "ko_sirt6": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_SIRT6/KO1_SIRT6/fastq/ko1_sirt6_b1_R1.fastq.gz",
                       "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_SIRT6/KO1_SIRT6/fastq/ko1_sirt6_b1_R2.fastq.gz"),
                      ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_SIRT6/KO2_SIRT6/fastq/ko2_sirt6_d1_R1.fastq.gz",
                       "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_SIRT6/KO2_SIRT6/fastq/ko2_sirt6_d1_R2.fastq.gz")],

         "wt_k56ac": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_K56AC/WT1_K56AC/fastq/wt1_k56ac_a3_R1.fastq.gz",
                       "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_K56AC/WT1_K56AC/fastq/wt1_k56ac_a3_R2.fastq.gz"),
                      ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_K56AC/WT2_K56AC/fastq/wt2_k56ac_c3_R1.fastq.gz",
                       "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_K56AC/WT2_K56AC/fastq/wt2_k56ac_c3_R2.fastq.gz")],

         "ko_k56ac": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_K56AC/KO1_K56AC/fastq/ko1_k56ac_b3_R1.fastq.gz",
                        "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_K56AC/KO1_K56AC/fastq/ko1_k56ac_b3_R2.fastq.gz"),
                       ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_K56AC/KO2_K56AC/fastq/ko2_k56ac_d3_R1.fastq.gz",
                        "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_K56AC/KO2_K56AC/fastq/ko2_k56ac_d3_R2.fastq.gz")]}

input_uva = {"wt_k9ac": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT1_IGG/fastq/wt1_igg_a4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT1_IGG/fastq/wt1_igg_a4_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT2_IGG/fastq/wt2_igg_c4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT2_IGG/fastq/wt2_igg_c4_R2.fastq.gz")],

         "ko_k9ac": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO1_IGG/fastq/ko1_igg_b4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO1_IGG/fastq/ko1_igg_b4_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO2_IGG/fastq/ko2_igg_d4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO2_IGG/fastq/ko2_igg_d4_R2.fastq.gz")],

         "wt_sirt6": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT1_IGG/fastq/wt1_igg_a4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT1_IGG/fastq/wt1_igg_a4_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT2_IGG/fastq/wt2_igg_c4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT2_IGG/fastq/wt2_igg_c4_R2.fastq.gz")],

         "ko_sirt6": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO1_IGG/fastq/ko1_igg_b4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO1_IGG/fastq/ko1_igg_b4_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO2_IGG/fastq/ko2_igg_d4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO2_IGG/fastq/ko2_igg_d4_R2.fastq.gz")],

         "wt_k56ac": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT1_IGG/fastq/wt1_igg_a4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT1_IGG/fastq/wt1_igg_a4_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT2_IGG/fastq/wt2_igg_c4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/WT_IGG/WT2_IGG/fastq/wt2_igg_c4_R2.fastq.gz")],

         "ko_k56ac": [("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO1_IGG/fastq/ko1_igg_b4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO1_IGG/fastq/ko1_igg_b4_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO2_IGG/fastq/ko2_igg_d4_R1.fastq.gz",
                      "/mnt/raid/zparteka/natalia_uva/novogene/usftp21.novogene.com/raw_data/samples/KO_IGG/KO2_IGG/fastq/ko2_igg_d4_R2.fastq.gz")]}

samples_yoruban = {
    "gm19238_CTCF": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_i/fastq/GM19238_CTCF_I_part2_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_i/fastq/GM19238_CTCF_I_part2_R2.fastq.gz"),
                     (
                         "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_ii/fastq/GM19238_CTCF_II_R1.fastq.gz",
                         "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/ctcf_ii/fastq/GM19238_CTCF_II_R2.fastq.gz")],
    "gm19239_CTCF": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/ctcf_i/fastq/GM19239_CTCF_I_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/ctcf_i/fastq/GM19239_CTCF_I_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/ctcf_ii/fastq/GM19239_CTCF_II_S2_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/ctcf_ii/fastq/GM19239_CTCF_II_S2_R2.fastq.gz")],
    "gm19240_CTCF": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/ctcf_i/fastq/GM19240_CTCF_I_S5_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/ctcf_i/fastq/GM19240_CTCF_I_S5_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/ctcf_ii/fastq/GM19240_CTCF_II_S6_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/ctcf_ii/fastq/GM19240_CTCF_II_S6_R2.fastq.gz")],
    "gm19238_smc1": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/smc1_i/fastq/GM19238_Smc1_I_part2_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/smc1_i/fastq/GM19238_Smc1_I_part2_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/smc1_ii/fastq/GM19238_Smc1_II_S1_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19238/smc1_ii/fastq/GM19238_Smc1_II_S1_R2.fastq.gz")],
    "gm19239_smc1": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/smc1_i/fastq/GM19239_Smc1_I_S3_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/smc1_i/fastq/GM19239_Smc1_I_S3_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/smc1_ii/fastq/GM19239_Smc1_II_S4_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19239/smc1_ii/fastq/GM19239_Smc1_II_S4_R2.fastq.gz")],
    "gm19240_smc1": [("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/smc1_i/fastq/GM19240_Smc1_I_S7_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/smc1_i/fastq/GM19240_Smc1_I_S7_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/smc1_ii/fastq/GM19240_Smc1_II_S8_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/yoruban/gm19240/smc1_ii/fastq/GM19240_Smc1_II_S8_R2.fastq.gz")]}

# on team-arwena
samples_chinese = {
    "hg00512_CTCF": [("/mnt/raid/zparteka/hichip/chinese/HG00512/ctcf_i/fastq/HG00512_CTCF_I_S2_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00512/ctcf_i/fastq/HG00512_CTCF_I_S2_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip/chinese/HG00512/ctcf_ii/fastq/HG00512_CTCF_II_S8_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00512/ctcf_ii/fastq/HG00512_CTCF_II_S8_R2.fastq.gz")],
    "hg00513_CTCF": [("/mnt/raid/zparteka/hichip/chinese/HG00513/ctcf_i/fastq/HG00513_CTCF_I_S3_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00513/ctcf_i/fastq/HG00513_CTCF_I_S3_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip/chinese/HG00513/ctcf_ii/fastq/HG00513_CTCF_II_S9_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00513/ctcf_ii/fastq/HG00513_CTCF_II_S9_R2.fastq.gz")],
    "hg00514_CTCF": [("/mnt/raid/zparteka/hichip/chinese/HG00514/ctcf_i/fastq/HG00514_CTCF_I_S4_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00514/ctcf_i/fastq/HG00514_CTCF_I_S4_R2.fastq.gz"),
                     ("/mnt/raid/zparteka/hichip/chinese/HG00514/ctcf_ii/fastq/HG00514_CTCF_II_S10_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip/chinese/HG00514/ctcf_ii/fastq/HG00514_CTCF_II_S10_R2.fastq.gz")]}

# on team-bilbo
samples_puerto = {
    "hg00731_CTCF": [("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_i/fastq/HG00731_CTCF_I_S5_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_i/fastq/HG00731_CTCF_I_S5_R2.fastq.gz"),
                     (
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_ii/fastq/HG00731_CTCF_II_S11_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00731/ctcf_ii/fastq/HG00731_CTCF_II_S11_R2.fastq.gz")],
    "hg00732_CTCF": [("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_i/fastq/HG00732_CTCF_I_S6_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_i/fastq/HG00732_CTCF_I_S6_R2.fastq.gz"),
                     (
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_ii/fastq/HG00732_CTCF_II_S12_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00732/ctcf_ii/fastq/HG00732_CTCF_II_S12_R2.fastq.gz")],
    "hg00733_CTCF": [("/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_i/fastq/HG00733_CTCF_I_S7_R1.fastq.gz",
                      "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_i/fastq/HG00733_CTCF_I_S7_R2.fastq.gz"),
                     (
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_ii/fastq/HG00733_CTCF_II_S12_R1.fastq.gz",
                     "/mnt/raid/zparteka/hichip_trios/puerto_rican/HG00733/ctcf_ii/fastq/HG00733_CTCF_II_S12_R2.fastq.gz")]
}
samples_mumbach = {"mumbach_cohesin": [("/mnt/raid/zparteka/hichip/mumbach/rep1/fastq/mumbach_rep1_pulled_R1.fastq",
                                        "/mnt/raid/zparteka/hichip/mumbach/rep1/fastq/mumbach_rep1_pulled_R2.fastq"),
                                       ("/mnt/raid/zparteka/hichip/mumbach/rep2/fastq/mumbach_rep2_pulled_R1.fastq",
                                        "/mnt/raid/zparteka/hichip/mumbach/rep2/fastq/mumbach_rep2_pulled_R2.fastq")]}

samples_gm12878 = {"gm12878_sa1": [("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_sa1_chip_seq/rep1/fastq/SRR998199_R1.fastq.gz",
                            "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_sa1_chip_seq/rep1/fastq/SRR998199_R2.fastq.gz"),
                           ("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_sa1_chip_seq/rep2/fastq/SRR998201_R1.fastq.gz",
                            "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_sa1_chip_seq/rep2/fastq/SRR998201_R2.fastq.gz")],
                   "gm12878_ctcf": [("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_ctcf_chip_seq/rep1/fastq/SRR998175_R1.fastq.gz",
                            "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_ctcf_chip_seq/rep1/fastq/SRR998175_R2.fastq.gz"),
                            ("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_ctcf_chip_seq/rep2/fastq/SRR998176_R1.fastq.gz",
                            "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_ctcf_chip_seq/rep2/fastq/SRR998176_R2.fastq.gz")]}
chips_gm_pooled = {"sample": [("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_sa1_chip_seq/gm12878_sa1_pulled/fastq/gm12878_sa1_pulled_R1.fastq.gz",
                               "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_sa1_chip_seq/gm12878_sa1_pulled/fastq/gm12878_sa1_pulled_R2.fastq.gz")]}
input_gm_pooled = {"sample": [("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_input/fastq/SRR998196_R1.fastq.gz",
                               "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_input/fastq/SRR998196_R2.fastq.gz")]}
input_gm12878 = {"gm12878_sa1": [("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_input/fastq/SRR998196_R1.fastq.gz",
                                  "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_input/fastq/SRR998196_R2.fastq.gz")],
                 "gm12878_ctcf": [("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_input/fastq/SRR998196_R1.fastq.gz",
                                   "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_input/fastq/SRR998196_R2.fastq.gz")]}
chips = chips_gm_pooled
input = input_gm_pooled
samples = samples_mumbach
sample_arima_hichip_shallow = {"sample": [("/mnt/raid/zparteka/cohesin_hichip_project/gm12878_shallow/fastq/KJ_Swift_1_R1.fastq.gz",
                                   "/mnt/raid/zparteka/cohesin_hichip_project/gm12878_shallow/fastq/KJ_Swift_1_R2.fastq.gz")]}
sample_arima_hichip_deep = {"sample": [("/mnt/raid/zparteka/cohesin_hichip_project/karolina_arima/raw_data/fastq/KJ_Swift_1_S12_R1.fastq.gz",
                                        "/mnt/raid/zparteka/cohesin_hichip_project/karolina_arima/raw_data/fastq/KJ_Swift_1_S12_R2.fastq.gz")]}
samples = sample_arima_hichip_deep

class Configuration:
    threads = 33
    # reference = "/mnt/raid/zparteka/mm10_genome/bwa/mm10.fa"
    reference = "/mnt/raid/zparteka/hg38_reference/Homo_sapiens_assembly38.fasta"
    mapq = 30
    peak_quality = 0.05
    genome_size = "hs"

    def __init__(self, r1, r2):
        self.r1 = r1
        self.r2 = r2
        self.outdir = r1.split("fastq")[0] + "luigi_seq_output/"
        if not isdir(self.outdir):
            makedirs(self.outdir)
        self.maps_dataset = basename(r1).split("_R")[0]
        self.fastq_dir = dirname(r1)
        # self.bwa_index = "/mnt/raid/zparteka/mm10_genome/bwa/mm10.fa"
        self.bwa_index = "/mnt/raid/zparteka/hg38_reference/Homo_sapiens_assembly38.fasta"
        self.outnames = {}
        self.create_outnames()
        self.narrow_peak = self.outnames["peaks"] + "_peaks.narrowPeak"

    # todo add checking if value is None
    def create_outnames(self):

        base = basename(self.r1.split("_R1")[0])
        self.outnames["mapped"] = join(self.outdir, f"{base}.bam")
        self.outnames["mapped_only"] = join(self.outdir, f"{base}_mapped.bam")
        self.outnames["filtered"] = join(self.outdir, f"{base}_mapped_filtered.bam")
        self.outnames["bigwig"] = join(self.outdir, f"{base}.bw")
        self.outnames["nodup"] = join(self.outdir, f"{base}_mapped_filtered_nodup.bam")
        self.outnames["index"] = join(self.outdir, f"{base}_indexed.bam")
        self.outnames["sorted"] = join(self.outdir, f"{base}_sorted.bam")
        self.outnames["peaks"] = join(self.outdir, f"{base}_macs3")
        # self.outnames["sam_flags"] = join(self.outdir, f"{base}_mapped_nodup_flags.bam")

    def dumps(self):
        c = {"r1": self.r1, "r2": self.r2}

        return c


def loads(p):
    c = Configuration(p["r1"], p["r2"])
    return c
