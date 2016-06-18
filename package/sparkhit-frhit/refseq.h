
/*
 * refseq.h for FR-HIT
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


#ifndef _REFSEQ_H_
#define _REFSEQ_H_

#include<vector>
#include<string>
#include<iostream>
#include<fstream>

#include "param.h"

using namespace std;

// Binary seq
struct OneBfa
{
    OneBfa()
    {
        n = 0;
    }

    bit32_t n;
    bit24_t *s;
};

// Ref seq name and size
struct RefTitle
{

    RefTitle()
    {
        name = "";
        size = 0;
    }

    string name;
    bit32_t size;
};

// Candidate block
struct Block
{

    Block()
    {
        id = 0;
        begin = 0;
        end = 0;
    }
    bit32_t id;
    bit32_t begin;
    bit32_t end;

};

// Qgram infor
struct KmerLoc
{

    KmerLoc()
    {
        n = 0;
    }

    bit32_t n;  // one kind of segment 
    ref_id_t *id;
    ref_loc_t *loc;
};

class RefSeq
{
    public:

        RefSeq() 
        {
            total_kmers = 0;
        }

        //release memory of bfa
        ~RefSeq()
        { 
            if (bfa.size()) 
                for (int i=0; i<bfa.size(); i++) 
                    delete[] bfa[i].s; 
        }

        //for input sequences
        int total_num;             //number of sequences//should use ref_id_t type

        bit64_t sum_length;        //total length of all sequences

        vector<OneBfa> bfa;        //transform _seq to binary sequences, then _seq is released
        vector<Block> _blocks;     //unmasked ref region, id is subscript of bfa
        vector<RefTitle> title;    //names and lengths of reference sequences
        
        ref_loc_t LoadNextSeq(ifstream &fin);  //read _seq and _length of one refseq

        void Run_ConvertBinseq(ifstream &fin); //read refseqs
        void BinSeq(OneBfa &a);    //transform string _seq to binary sequence and store in a
        void UnmaskRegion();       //break refseq into ACGT blocks, not include NX, but included if len(NX)<5
        
        //for make index
        bit32_t total_kmers;       //4^k
        KmerLoc *index;            //will have 4^k kmer elements
        
        void CreateIndex();        //entry
        void InitialIndex();       //get total_kmers and memory for index
        void t_CalKmerFreq_ab();   //get frequency of kmers, only KmerLoc.n changed
        void AllocIndex();
        void t_CreateIndex_ab();
        void ReleaseIndex();       //unused

    protected:

        string _name;        //fasta sequence name   //go into title
        string _seq;         //temporary sequence storage, released after generated binary sequence  //go into bfa

        ref_loc_t _length;   //fasta sequence length //go into title 
        ref_id_t _count;     //temporary sequence id number, could be replaced by total_num //go into _blocks
};

#endif //_REFSEQ_H_
