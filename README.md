# luigi_seq

## IMPORTANT
This is a legacy version of currently recommended **nf-HiChIP** pipeline. Please use the current version available here: https://github.com/SFGLab/nf-hichip
The current version of the pipeline is described in our bioRxiv paper: https://www.biorxiv.org/content/10.1101/2024.05.16.594268v1
## Required file structure
Required structure of the input data. 


```
Data
└───sample 1
│   └───replicate 1
│   │   └───fastq
│   │   │   file1_R1.fastq.gz
│   │   │   file1_R2.fastq.gz
│   └───replicate 2
│       └───fastq
│       │   file2_R1.fastq.gz
│       │   file2_R2.fastq.gz
└───sample 2
│   └───replicate 1
│   │   └───fastq
│   │   │   file1_R1.fastq.gz
│   │   │   file1_R2.fastq.gz
│   └───replicate 2
│       └───fastq
│       │   file2_R1.fastq.gz
│       │   file2_R2.fastq.gz  
└───input1 
│   └───replicate 1 
│   │   └───fastq
│   │   │   file1_R1.fastq.gz
│   │   │   file1_R2.fastq.gz
│   └───replicate 2
│       └───fastq
│       │   file2_R1.fastq.gz
│       │   file2_R2.fastq.gz    
    ...
```