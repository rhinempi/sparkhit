
/*
 * param.cpp for FR-HIT
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

#include "param.h"
#include<iostream>
#include<bitset>
using namespace std;

//Aa=0 Cc=1 Gg=2 Tt=3
bit8_t alphabet[256];

void initalphabet()
{  
   //convert unknown char as 'A'
   for(int i=0; i<256; i++)
   {
       alphabet[i] = 0;
   }

   alphabet['c']=alphabet['C']=1;
   alphabet['g']=alphabet['G']=2;
   alphabet['t']=alphabet['T']=3;
}

bit8_t rev_alphabet[256];

void initrevalphabet()
{ 
   //convert unknown char as 'T' | complementary chain 
   for(int i=0; i<256; i++)
   {
       rev_alphabet[i] = 3;
   }

   rev_alphabet['c']=rev_alphabet['C']=2;
   rev_alphabet['g']=rev_alphabet['G']=1;
   rev_alphabet['t']=rev_alphabet['T']=0;

}

bit8_t nvfilter[256];

void initnvfilter()
{ //NNNNNN filtering on reference genome
   for(int i=0; i<256; i++) nvfilter[i] = 1;
   nvfilter['a']=nvfilter['A']=0;
   nvfilter['c']=nvfilter['C']=0;
   nvfilter['g']=nvfilter['G']=0;
   nvfilter['t']=nvfilter['T']=0;
}

char chain_flag[2] = {'+', '-'};
char nt_code[4] = {'A', 'C', 'G', 'T'};
char revnt_code[4] = {'T', 'G', 'C', 'A'};

Param::Param()
{
    ncpu = 1;
    chains = 0;
    max_dbseq_size = 0x1000000; //16Mb
    append_dbseq_size = 0x1000000; //16Mb

    fin_q = false;

    seed_size = 12;
    seed_bits = (1<<(seed_size*2))-1;
    seed_overlap = 8; //index style | seed overlap 

    min_read_size = 20;
    identity = 75;
    global = 0;
    align_len = 30;
    band = 4;
    max_read_size = 5000; //read reading
    append_read_size = 5000; //read reading append
    report_repeat_hits = 0; //how to report the hits result

    //repeats mask part	
    mask = 1; //repeats mask?
    useful_nt = "ACGT"; //for mask area filtering 
    nx_nt = "NXacgt"; //for mask area filtering
    useful_nt_nomask = "ACGTacgt"; //no mask
    nx_nt_nomask = "NX"; //only mask NX

    best_pigeon = 1; // at least 1 hit to form a block
    best_kmers = 20; //4-mers
    best_nas = 24; //bps 80%
    maxtrys = 20; //tries for alignment
    max_align_hits = 0; //debug only ...
    lenforstep = 1000; //2bp step cutoff | for Sanger long reads
    global_signal = 1; //global alignment control
    global_part = 90; //don't need that anymore
    outputformat = 0; //FR-HIT default format
    evalue = 10; //default evalue cutoff
    
    initalphabet();
    initrevalphabet();
    initnvfilter();

};

void Param::SetSeedSize(int n) 
{
    seed_size = n;
    seed_bits = (1<<(seed_size*2))-1;
#ifdef DEBUG
    bitset <32> bs2((long)seed_bits);
    cerr<<" " <<bs2<<endl;
    bs2.reset();
#endif

}

