
/*
 * refseq.cpp for FR-HIT
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


#include<iostream>
#include<bitset>
#include<omp.h>
#include "utilities.h"
#include "refseq.h"

//#define DEBUG
#ifdef DEBUG
//for binary data test
bitset <24> bs1((long)a.s[i].a);
cerr<<bs1;   
bs1.reset();
#endif

using namespace std;

extern Param param;
extern bit8_t alphabet[];

ref_loc_t RefSeq::LoadNextSeq(ifstream &fin)
{

    char ch[1000];
    char c;

    string s;
    
    fin>>c;
    if (fin.eof()) return 0;
    _length = 0;

    //get name
    fin>>_name;
    fin.getline(ch, 1000);

    //get seq
    while (!fin.eof())
    {
        fin>>c; 
        if (fin.eof()) break;
        fin.unget();
        if (c == '>') break;
        fin>>s;
        if (_length+s.size() >= param.max_dbseq_size)
        {
            param.max_dbseq_size+=param.append_dbseq_size;
            _seq.resize(param.max_dbseq_size);
        }

        copy(s.begin(), s.end(), _seq.begin()+_length);
        _length += s.size();

    }

    return _length;
}

void RefSeq::BinSeq(OneBfa &a)
{
    //struct OneBfa{bit32_t n;     bit24_t *s;};
    //12bp, bit24 for each element. put 2 extra elements at the 3'end to avoid overflow
    a.n = (_length+11)/12+2;
    int t = a.n*12-_length;
    if(t) 
    {
        string ts(t, 'N');
        if (_seq.size()<_length+t) _seq.resize(_length+t);
        copy(ts.begin(), ts.end(), _seq.begin()+_length);
    }
    a.s = new bit24_t[a.n];//still use memory amount equivalent to bit32

    string::iterator p=_seq.begin();

    for (bit32_t i=0; i<a.n; i++,p+=12)
    {
        a.s[i].a = 0;

        for (bit32_t j=0; j<12; j++)
        {
            a.s[i].a<<=2;
            a.s[i].a|=alphabet[*(p+j)];
        }
    }

}

// New added maskregion
void RefSeq::UnmaskRegion()
{
    Block b;

    b.id=_count;
    b.begin=b.end=0;

    while (b.end < _length)
    {
        b.begin=_seq.find_first_of(param.useful_nt, b.end);

        if (b.begin > _length) break;

        b.end = _seq.find_first_of(param.nx_nt, b.begin);
        b.end = (b.end<=_length? b.end : _length);

        //ignore 5 Ns
        if ((!_blocks.empty()) && (b.id==_blocks[_blocks.size()-1].id) && (b.begin - _blocks[_blocks.size()-1].end <5)) _blocks[_blocks.size()-1].end = b.end;
        else
        {
            // filtering short segment
            if (b.end-b.begin < 20) continue;
            _blocks.push_back(b);
        }
    } // end while
}

// convert to binary seq
void RefSeq::Run_ConvertBinseq(ifstream &fin)
{
    _seq.resize(param.max_dbseq_size);
    RefTitle r;
    _count = 0; 
    total_num = sum_length = 0;

    while (LoadNextSeq(fin))
    {
        //filtering little reference sequences
        if (_length < 20) continue;
        // input one seq in ref file
        r.name = _name;
        r.size = _length;
        title.push_back(r);
        OneBfa a;
        bfa.push_back(a);
        BinSeq(bfa[bfa.size()-1]);
        UnmaskRegion();
        _count++;
        total_num++;
        sum_length+=_length;
    }

    _seq.clear(); //free ram
}

void RefSeq::InitialIndex()
{
    total_kmers = 1<<(param.seed_size*2);
    index = new KmerLoc[total_kmers];
    for (bit32_t i=0; i<total_kmers; i++)
        index[i].n = 0;

}

void RefSeq::t_CalKmerFreq_ab()
{
    bit24_t *_m;
    bit32_t i,b,temp_seed;
    bit32_t step(param.seed_size-param.seed_overlap);

    for (vector<Block>::iterator p=_blocks.begin(); p!=_blocks.end(); p++) 
    {
        _m = bfa[p->id].s;

        // param.seed_overlap: index step 
        for (i=p->begin; i< (p->end - param.seed_size + 1); i+=step) 
        { 
            b = (i%12 + param.seed_size)*2;
            temp_seed = b<=24? (_m[i/12].a>>(24-b))&param.seed_bits : (_m[i/12].a<<(b-24)|_m[i/12+1].a>>48-b)&param.seed_bits;
            index[temp_seed].n++;
#ifdef DEBUG
            bitset <22> bs1((long)temp_seed);cerr<<bs1<<endl;bs1.reset();
#endif
        }

        //add last q-gram under
        if (((p->end-p->begin)-param.seed_size)%step) 
        {
            i = p->end - param.seed_size;
            b = (i%12 + param.seed_size)*2;
            temp_seed = b<=24? (_m[i/12].a>>(24-b))&param.seed_bits : (_m[i/12].a<<(b-24)|_m[i/12+1].a>>48-b)&param.seed_bits;
            index[temp_seed].n++;
        }
    }
}

void RefSeq::AllocIndex() 
{
    KmerLoc *v = index;
    for (bit32_t j=0; j<total_kmers;j++,v++)
        if (v->n>0) 
        {
            v->id = new ref_id_t[v->n];
            v->loc = new ref_loc_t[v->n];
            v->n = 0;
        }
}

void RefSeq::ReleaseIndex() 
{
    for (bit32_t j=0; j<total_kmers; j++) 
    {
       delete[] index[j].id;
       delete[] index[j].loc;
    }
    delete[] index;

}

// ref index
void RefSeq::t_CreateIndex_ab() 
{
    bit24_t *_m;
    bit32_t i,b,temp_seed;

    KmerLoc *z;
    bit32_t step(param.seed_size-param.seed_overlap);

    for (vector<Block>::iterator p=_blocks.begin(); p!=_blocks.end(); p++) 
    {
        _m = bfa[p->id].s;

        for (i=p->begin; i< (p->end - param.seed_size + 1); i+=step) 
        { 
            b = (i%12 + param.seed_size)*2;
            temp_seed = b<=24? (_m[i/12].a>>(24-b))&param.seed_bits : (_m[i/12].a<<(b-24)|_m[i/12+1].a>>48-b)&param.seed_bits;
            z = index + temp_seed;
            z->loc[z->n] = i;
            z->id[z->n] = p->id;
            z->n++;
        }

        //add last q-gram under 
        if (((p->end-p->begin)-param.seed_size)%step) 
        {
            i = p->end - param.seed_size;
            b = (i%12 + param.seed_size)*2;
            temp_seed = b<=24? (_m[i/12].a>>(24-b))&param.seed_bits : (_m[i/12].a<<(b-24)|_m[i/12+1].a>>48-b)&param.seed_bits;
            z = index + temp_seed;
            z->loc[z->n] = i;
            z->id[z->n] = p->id;
            z->n++;
        }
    }

}

//assign memory first and then index loading
void RefSeq::CreateIndex() 
{
    InitialIndex();
    t_CalKmerFreq_ab();
    AllocIndex();
    t_CreateIndex_ab();
}

