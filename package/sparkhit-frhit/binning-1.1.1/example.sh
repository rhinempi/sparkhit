#!/bin/sh

frhit -a example.fasta -d bacteria.fasta -o example.frhit
python binning.py example.frhit example.binning
