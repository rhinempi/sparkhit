
/*
 * param.h for FR-HIT
 * Copyright (c) 2010-2011 Beifang Niu All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef _PARAM_H_
#define _PARAM_H_

#include<string>

using namespace std;

typedef unsigned char bit8_t;
typedef unsigned short bit16_t;
typedef unsigned bit32_t;
typedef unsigned long long bit64_t;

//24bits unit
struct bit24_t
{
    unsigned a:24;
};

// seqs<4G, length <4G
typedef bit32_t ref_id_t;
typedef bit32_t ref_loc_t;

class Param
{
    public:
        
        Param();
        void SetSeedSize(int n);

    public:

        int ncpu;            //-T number of parallel processors
        int chains;          //-n 0: both; 1: direct only; 2: complementary only
        int max_dbseq_size;  //default 16M dbseq
        int append_dbseq_size;  //default 16M
	bool fin_q;
        int seed_size;       //-k default 11[8,12]
        int seed_overlap;    //-p default 8
        int min_read_size;   //-l default 20
        int identity;        //-c default 80
        int global;          //default 0
        int align_len;       //-m default 30 when -g=0
        int band;            //-b default 4
        int max_read_size;   //default 600 for reads reading
        int append_read_size;//default 200
        int report_repeat_hits;    //-r default 0, how report repeat hits? 0:all; N:the best top N hits for one read[?0: no, 1: pick one randomly, 2: report all?]
	int best_pigeon;
        int best_kmers;      //default 20, 4-mers
        int best_nas;        //default 24, bps needed
        int max_align_hits;  //default 0
        int maxtrys;         //-t default 20, max alignment attemps
        int global_signal;   //-g default 0, 1 for global alignment control
        int global_part;     //-q default 90 when -g=1
        int lenforstep;      //-w default 1000, for 454 long reads using 2bp q-gram index step
        int outputformat;    //-f default 0, format control for output file, 0:FR-HIT format; 1:PSL fromat
        int mask;						 //-u default 1, mask out repeats as lower cased sequence to prevent spurious hits? 1: yes; 0: no
        double evalue;			 // -e default 0.001, evalue cutoff;
        string useful_nt;    //mask lower base except "ACGT", alphabet table
        string nx_nt;        //mask "NXacgt"
        string useful_nt_nomask; //no mask
        string nx_nt_nomask; //only mask "NX"

        bit8_t num_mismatch[0xffffff]; //mismatch table
        bit32_t seed_bits;   //mask for seed(kmer)

};

#endif //_PARAM_H_
