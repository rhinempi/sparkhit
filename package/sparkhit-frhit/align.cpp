
/*
 * align.cpp for FR-HIT
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

#include<algorithm>
#include<cmath>
#include<bitset>
#include<omp.h>

#include "align.h"

using namespace std;

extern bit8_t alphabet[];

extern bit8_t reg_alphabet[];
extern bit8_t rev_alphabet[];

// Reads Ns filtering
extern bit8_t nvfilter[];

extern char rev_char[];
extern char chain_flag[];

extern char nt_code[];
extern char revnt_code[];

// For alignment DP
int BLOSUM62[] = 
{
  4,                                                                  // A
 -1, 5,                                                               // R
 -2, 0, 6,                                                            // N
 -2,-2, 1, 6,                                                         // D
  0,-3,-3,-3, 9,                                                      // C
 -1, 1, 0, 0,-3, 5,                                                   // Q
 -1, 0, 0, 2,-4, 2, 5,                                                // E
  0,-2, 0,-1,-3,-2,-2, 6,                                             // G
 -2, 0, 1,-1,-3, 0, 0,-2, 8,                                          // H
 -1,-3,-3,-3,-1,-3,-3,-4,-3, 4,                                       // I
 -1,-2,-3,-4,-1,-2,-3,-4,-3, 2, 4,                                    // L
 -1, 2, 0,-1,-3, 1, 1,-2,-1,-3,-2, 5,                                 // K
 -1,-1,-2,-3,-1, 0,-2,-3,-2, 1, 2,-1, 5,                              // M
 -2,-3,-3,-3,-2,-3,-3,-3,-1, 0, 0,-3, 0, 6,                           // F
 -1,-2,-2,-1,-3,-1,-1,-2,-2,-3,-3,-1,-2,-4, 7,                        // P
  1,-1, 1, 0,-1, 0, 0, 0,-1,-2,-2, 0,-1,-2,-1, 4,                     // S
  0,-1, 0,-1,-1,-1,-1,-2,-2,-1,-1,-1,-1,-2,-1, 1, 5,                  // T
 -3,-3,-4,-4,-2,-2,-3,-2,-2,-3,-2,-3,-1, 1,-4,-3,-2,11,               // W
 -2,-2,-2,-3,-2,-1,-2,-3, 2,-1,-1,-2,-1, 3,-3,-2,-2, 2, 7,            // Y
  0,-3,-3,-3,-1,-2,-2,-3,-3, 3, 1,-2, 1,-1,-2,-2, 0,-3,-1, 4,         // V
 -2,-1, 3, 4,-3, 0, 1,-1, 0,-3,-4, 0,-3,-3,-2, 0,-1,-4,-3,-3, 4,      // B
 -1, 0, 0, 1,-3, 3, 4,-2, 0,-3,-3, 1,-1,-3,-1, 0,-1,-3,-2,-2, 1, 4,   // Z
  0,-1,-1,-1,-2,-1,-1,-1,-1,-1,-1,-1,-1,-1,-2, 0, 0,-2,-1,-1,-1,-1,-1 // X
//A  R  N  D  C  Q  E  G  H  I  L  K  M  F  P  S  T  W  Y  V  B  Z  X
//0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19  2  6 20
};

int na2idx[] = {0, 4, 1, 4, 4, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 4, 4, 4, 4, 4};
// idx for  A  B  C  D  E  F  G  H  I  J  K  L  M  N  O  P
//          Q  R  S  T  U  V  W  X  Y  Z
// so aa2idx[ X - 'A'] => idx_of_X, eg aa2idx['A' - 'A'] => 0,
// and aa2idx['M'-'A'] => 4

int BLOSUM62_na[] = 
{
    1,                // A
    -2, 1,            // C
    -2,-2, 1,         // G
    -2,-2,-2, 1,      // T
    -2,-2,-2, 1, 1,   // U
    -2,-2,-2,-2,-2, 1 // N
        //A  C  G  T  U  N
        //0  1  2  3  3  4
};

void AA_MATRIX::init() 
{
    gap = -11;
    ext_gap = -1;

    for (int i=0; i<MAX_GAP; i++)
    {
        gap_array[i] = gap + i * ext_gap;
    }

    int k = 0;

    for (int i=0; i<MAX_AA; i++)
    {
        for ( int j=0; j<=i; j++)
        {
            matrix[j][i] = matrix[i][j] = BLOSUM62[k++];
        }
    }

} // END void AA_MATRIX::init()

void AA_MATRIX::set_gap(int gap1, int ext_gap1) 
{
    gap = gap1;
    ext_gap = ext_gap1;

    for (int i=0; i<MAX_GAP; i++)
    {
        gap_array[i] = gap + i * ext_gap;
    }

} // END void AA_MATRIX::set_gap

void AA_MATRIX::set_matrix(int *mat1) 
{
    int k = 0;

    for (int i=0; i<MAX_AA; i++)
    {
        for (int j=0; j<=i; j++)
        {
            matrix[j][i] = matrix[i][j] = mat1[k++];
        }
    }

} // END void AA_MATRIX::set_matrix

void AA_MATRIX::set_to_na() 
{
    gap = -6;
    ext_gap = -1;

    for (int i=0; i<MAX_GAP; i++)
    {
        gap_array[i] = gap + i * ext_gap;
    }

    int k = 0;

    for (int i=0; i<MAX_NA; i++)
    {
        for (int j=0; j<=i; j++)
        {
            matrix[j][i] = matrix[i][j] = BLOSUM62_na[k++];
        }
    }
} // END void AA_MATRIX::set_to_na

// Assign memory for DP alignment variables
void WorkingPara::init()
{
    for (int i=0; i<_MaxPartLen; i++)
    {
        score_mat[i] = new int [_MaxPartLen];
        iden_mat[i]  = new int [_MaxPartLen];
        from1_mat[i] = new int [_MaxPartLen];
        from2_mat[i] = new int [_MaxPartLen];
        alnln_mat[i] = new int [_MaxPartLen];
    }

    align_len=param.align_len;
    best_nas=param.best_nas;
    best_kmers=param.best_kmers;
}

void WorkingPara::final()
{
    // Release dp variables
    for (int i=0; i<_MaxPartLen; i++)
    {
        delete [] score_mat[i]; 
        delete [] iden_mat[i];
        delete [] from1_mat[i];
        delete [] from2_mat[i];
        delete [] alnln_mat[i];
    }
}

// Create seed profile
ReadAlign::ReadAlign()
{
    // count aligned reads num 
    n_aligned = 0;
    // cutoff parameters
    band_width = param.band; 
}

// Entry point for parallel 
void ReadAlign::Do_Batch(RefSeq &ref, ReadClass &read_a, ifstream &fin_a, ofstream &fout)
{
    _str_align.clear();
    bit32_t naligned(0);
    vector<ReadInf>::iterator _pread;
    WorkingPara wp;

    int morereads(1);
    int iam=0;
    int np=1;

    // Parallel entry point
#pragma omp parallel private(_pread, wp)
    {
#if defined (_OPENMP)
        iam = omp_get_thread_num();
#pragma omp single
        if (param.ncpu == 0)
        {
            param.ncpu=omp_get_num_threads();
        }
        //cerr<<param.ncpu<<" cpu used"<<endl;
        //np = omp_get_num_threads();
#endif
        while (morereads)
        {
#pragma omp barrier
#pragma omp single
            {
		if (param.fin_q == true){
			morereads = read_a.LoadBatchFastqReads(fin_a);
			ImportBatchReads(read_a.num, &read_a.mreads);
		}else{
                	morereads = read_a.LoadBatchReads(fin_a);
                	ImportBatchReads(read_a.num, &read_a.mreads);
		}
            }
            if (morereads)
            {
                naligned=0;
#pragma omp for schedule(dynamic, 100) reduction(+:naligned)
                for (long tt=0; tt<num_reads; tt++)
                {
                    _pread = pmreads->begin() + tt;
                    // min read filtering
                    if (_pread->seq.size() >= param.min_read_size)
                    {
                        //-g=1 (global), using new filtering cutoff
                        if (param.global_signal == 1)
                        {
                            //wp.align_len = (_pread->length*param.global_part)/100;//nbps
                            wp.align_len = _pread->length;
                            wp.best_nas = (wp.align_len*param.identity)/100;
                            wp.best_kmers = wp.align_len - (wp.align_len-wp.best_nas)*4 - 3;//4-mers
			    if (param.identity >= 95) {
                                wp.bestPigeon = wp.align_len / param.seed_size - 1 - (wp.align_len - wp.best_nas);
                                if (wp.bestPigeon < 1) wp.bestPigeon = 1;
                            }
                        }
                        //Ns filtering
                        if (ConvertBinaySeq(_pread, wp) == 0)
                        { 
                            GenerateSeeds(_pread->length, wp);
                            if (AlignProcess(ref, _pread, str_align[iam], wp)) naligned++;
                        }
                    }
                }
#pragma omp single
                {
                    for (int i=0; i<param.ncpu; i++)
                    {
                        fout<<str_align[i]; //result write
                        str_align[i].clear();
                    }
                    cerr << read_a._index << " reads finished. " << Cal_AllTime() << " secs passed" << endl;
                    n_aligned += naligned;
                }

            } //end of if

        } //end of while

    } //end of parallel part

}

// Convert string seq to binary type
int ReadAlign::ConvertBinaySeq(vector<ReadInf>::iterator &_pread, WorkingPara &wp)
{   
    int i,j,h,g;
    int nxs =0;

    bit24_t _a;

    string::iterator _sp;
    string::reverse_iterator _sq;

    // Direct chain
    h = _a.a = 0;
    for (_sp = _pread->seq.begin(), i = 1; _sp!=_pread->seq.end(); _sp++, i++)
    {
        _a.a<<=2;
        _a.a|=alphabet[*_sp];

        // Assign iseq value 
        wp.iseqquery[i-1] = alphabet[*_sp];
        nxs += nvfilter[*_sp];

        if (0 == i%12)
        {
            wp.bseq[h] = _a;
#ifdef DEBUG
            cerr<<" h "<<h<<endl;
            bitset <24> bs((long)wp.bseq[h].a);
            cerr<<" " <<bs<<endl;
            bs.reset();
#endif
            h++;
            _a.a = 0;
        }

    }

    if (nxs >= wp.best_nas) return 1;
    if (_pread->length%12) 
    {
        _a.a<<=24-(_pread->length%12)*2;
        wp.bseq[h]=_a;
    }
    
    // Reverse seq
    h = _a.a = 0;
    for (_sq=_pread->seq.rbegin(), i=1; _sq != _pread->seq.rend(); _sq++, i++)
    {
        _a.a<<=2;
        _a.a|=rev_alphabet[*_sq];

        // Assign ciseq value (reverse strain) 
        wp.ciseqquery[i-1]=rev_alphabet[*_sq];
        if (0 == i%12)
        {
            wp.cbseq[h] = _a;
            h++;
            _a.a=0;
        }
    }

    if (_pread->length%12)
    { 
        _a.a<<=24-(_pread->length%12)*2;
        wp.cbseq[h] = _a;
    }
    // test iseq and ciseq (only debug)
#ifdef DEBUG
    cerr << endl;
    for (i=0; i<_pread->length; i++)
    {
        cerr << int(iseqquery[i]);
    }
    cerr << endl;
#endif

    return 0;

}

// Get seeds of reads one base one seed
void ReadAlign::GenerateSeeds(int _read_length, WorkingPara &wp)
{
    int j(_read_length - param.seed_size + 1);
    for (int i=0; i<j; i++)
    { 
        int b = (i%12 + param.seed_size)*2;  //some kind of end position
        wp.seeds[i] = b<=24? ( wp.bseq[i/12].a>>(24-b))&param.seed_bits : (wp.bseq[i/12].a<<(b-24)| wp.bseq[i/12+1].a>>48-b)&param.seed_bits;
        wp.cseeds[i] = b<=24? (wp.cbseq[i/12].a>>(24-b))&param.seed_bits : (wp.cbseq[i/12].a<<(b-24)|wp.cbseq[i/12+1].a>>48-b)&param.seed_bits;
#ifdef DEBUG
        bitset <26> bs2((long)cseeds[i]);
        cerr << " " << bs2 << endl;
        bs2.reset();
#endif 
    }

}

// OOP redo
void ReadAlign::AhitCollect(int _read_length, int seedstep, RefSeq &ref, bit32_t *seeds, vector<bit64_t> &Ahit)
{
    int temp_loc;

    bit32_t m,j,seed;
    bit64_t Ta_hit;

    ref_id_t *_refid;
    ref_loc_t *_refloc;

    Hit _hit;
    // Seed num of read 
    bit32_t snum(_read_length - param.seed_size + 1);
    for(int i=0; i<snum; i+=seedstep)
    {
        seed = seeds[i]; // bit32_t

        if ((m=ref.index[seed].n) == 0) continue; //no match

        // List of seeded reference locations
        _refid = ref.index[seed].id; 
        _refloc = ref.index[seed].loc;

        for (j=0; j!= m; j++)
        {
            // Seed location
            _hit.chr = _refid[j]; 
            // Start position of possible alignment to read
            temp_loc = _refloc[j] - i;
            // Beyond the boundary( ref: 0)
            _hit.loc = temp_loc >= 0 ? temp_loc : 0;
            // Load refid and location in one 64 bit
            Ta_hit = ((bit64_t)_refid[j]<<32)|_hit.loc;
            Ahit.push_back(Ta_hit);
        } 
    }
}

// OOP redo
void ReadAlign::HitcansCollect(bit64_t tcc, RefSeq &ref, int _read_length, WorkingPara &wp)
{
    int t_loc;

    bit32_t ccflag;
    bit64_t tcc0;

    ref_loc_t ttloc;
    Hit_Can Hitcan; //alignment hit sample
    ccflag = 0;
    for (bit32_t d = 1; d< wp.Ahit.size(); d++)
    {
        // vary in seed_size length scope
        if ((wp.Ahit[d] - tcc) <= param.seed_size)
        {
            if (ccflag == 0)
            { //get id and loc
                Hitcan.chr = tcc>>32;
                tcc0 = tcc<<32;
                ttloc = tcc0>>32;
                t_loc = ttloc - param.seed_size;
                // get alignment location info
                Hitcan._begin = t_loc > 0 ? t_loc : 0;
                // alignlength = read length add 2 folds seedsize 
                Hitcan._end = ttloc + _read_length + 2*param.seed_size;
                if (Hitcan._end > (ref.title[Hitcan.chr].size-1)) Hitcan._end = ref.title[Hitcan.chr].size;
                wp._hit_Cans.push_back(Hitcan);
                ccflag = 1;
            }
        }
        else
        {
            tcc = wp.Ahit[d];
            ccflag = 0;
        }
    }
}

void ReadAlign::HitcansCollectPigeon(bit64_t tcc, RefSeq &ref, int _read_length, WorkingPara &wp)
{
    int t_loc;

    bit32_t ccflag;
    bit32_t pigeonHit;
    bit64_t tcc0;

    ref_loc_t ttloc;
    Hit_Can Hitcan; //alignment hit sample
    ccflag = 0;

    pigeonHit =0;
    for (bit32_t d = 1; d< wp.Ahit.size(); d++)
    {
        // vary in seed_size length scope
        if ((wp.Ahit[d] - tcc) <= param.seed_size)
        {
            pigeonHit++;
            if (ccflag == 0 && pigeonHit >= wp.bestPigeon)
            { //get id and loc
                Hitcan.chr = tcc>>32;
                tcc0 = tcc<<32;
                ttloc = tcc0>>32;
                t_loc = ttloc - param.seed_size;
                // get alignment location info
                Hitcan._begin = t_loc > 0 ? t_loc : 0;
                // alignlength = read length add 2 folds seedsize
                Hitcan._end = ttloc + _read_length + 2*param.seed_size;
                if (Hitcan._end > (ref.title[Hitcan.chr].size-1)) Hitcan._end = ref.title[Hitcan.chr].size;
                wp._hit_Cans.push_back(Hitcan);
                ccflag = 1;
            }
        }
        else
        {
            tcc = wp.Ahit[d];
            ccflag = 0;
            pigeonHit = 0;
        }
    }
}

// Merge adjoining blocks
void ReadAlign::MergeAdjoiningBlock(WorkingPara &wp)
{
    int d;
    Hit_Can Hitcan;

    // Alignment candidates filtering
    if (wp._hit_Cans.size() > 1)
    {
        Hitcan = wp._hit_Cans[0];
        for (d=1; d< wp._hit_Cans.size(); d++) 
        {
            if ((wp._hit_Cans[d].chr!=Hitcan.chr) || (int(Hitcan._end-wp._hit_Cans[d]._begin) < 0)) 
            {
                //not same chr, or are apart
                wp.perfect_hit.push_back(Hitcan);
                Hitcan = wp._hit_Cans[d];
            }
            else
            {
                //if overlap, merge to extend it
                //ref part length limit MAX_READ
                //maybe lost some alignment but it is enough
                if ((Hitcan._end - Hitcan._begin) < MAX_READ) Hitcan._end = wp._hit_Cans[d]._end;
            }
        }
        wp.perfect_hit.push_back(Hitcan);
    }
    else if (wp._hit_Cans.size() == 1)
    {
        //only one hit 
        wp.perfect_hit.push_back(wp._hit_Cans[0]);
    }

}

// 4-mers checking
void ReadAlign::CreateFourmers(char *iseqquery, int _read_length, WorkingPara &wp) 
{
    int j, sk, mm,c22;
    int lencc = _read_length - 3;

    // Get 4-mers value of query
    for (sk=0; sk<NAA4; sk++)
    {
        wp.taap[sk] = 0;
    }

    for (j=0; j<lencc; j++)
    {
        c22 = iseqquery[j]*NAA3 + iseqquery[j+1]*NAA2 + iseqquery[j+2]*NAA1 + iseqquery[j+3];
        wp.taap[c22]++;
    }

    // Make index
    for (sk=0,mm=0; sk<NAA4; sk++) 
    {
        wp.aap_begin[sk] = mm; 
        mm += wp.taap[sk]; 
        wp.taap[sk] = 0;
    }

    for (j=0; j<lencc; j++) 
    {
        c22 = iseqquery[j]*NAA3 + iseqquery[j+1]*NAA2 + iseqquery[j+2]*NAA1 + iseqquery[j+3];
        wp.aap_list[wp.aap_begin[c22]+wp.taap[c22]++] = j;
    }

}

// Main filtering and alignment part 
int ReadAlign::AlignProcess(RefSeq &ref, vector<ReadInf>::iterator &_pread, string &os, WorkingPara &wp) 
{
    // e-value
    int eHSP = expectedHSPlength(mink,_pread->length,ref.sum_length,Hh);

    // expect HSP length for e-value computing
    int eLengthSeq = effectiveLengthSeq(_pread->length,eHSP,mink);

    bit64_t eLengthDB = effectiveLengthDb(ref.sum_length,eHSP,ref.total_num,mink);

    double eValue;
    double read_identity_d;

    int trys;
    int seedstep;
    int repreports;

    int len;
    int len1;
    int best_sum;

    int RefBegin;
    int RefEnd;
    int alnln;

    int tiden_no;
    int read_cov;
    int read_identity;
    int talign_info[5];
    
    // Alignment and filtering
    int band_left;
    int band_right; 
    int _RecruitedSignal(0);
    int best_score;

    Hit_Can Hitcan;

    bit24_t *_m;
    bit32_t i,j,d,b;
    bit64_t tcc;

    OutputSort Outputunit;

    // Output sorting
    vector<OutputSort> Outputlist;

    // Get part of ref seq for alignment
    char seqjseg[_MaxPartLen]; 

    // Result output container
    char _ch[1000];

    MerSort Gramunit;
    vector< MerSort > Gramsort; //qgrams sorting
    seedstep = 1;//speedup using step q-gram of query read
    if (_pread->length >= param.lenforstep) seedstep = 2;

    // Alignment and 4 mers counting
    len = _pread->length;

    // 0:both; 1:direct only; 2:complementary only. default=0 direct chain
    if ((0 == param.chains) ||(1 == param.chains))
    {
        AhitCollect(_pread->length, seedstep, ref,  wp.seeds, wp.Ahit);

        // get alignment candidates
	if (param.identity >= 95){
            if (wp.Ahit.size() >= wp.bestPigeon){
                sort(wp.Ahit.begin(), wp.Ahit.end());
                tcc = wp.Ahit[0];
                HitcansCollectPigeon(tcc, ref, _pread->length, wp);
            }
        }else {
            if (wp.Ahit.size() > 1)
            {
            	sort(wp.Ahit.begin(),wp.Ahit.end());
           	tcc = wp.Ahit[0];
            	HitcansCollect(tcc, ref, _pread->length, wp);
            }
	}

        wp.Ahit.resize(0); // Release memory
        MergeAdjoiningBlock(wp); // Merge adjoining blocks
        wp._hit_Cans.resize(0); 

        // get 4-mers value of query
        CreateFourmers(wp.iseqquery, _pread->length, wp);

        // for 4-kmers count filtering and alignment 
        for (d = 0; d< wp.perfect_hit.size(); d++)
        {
            Hitcan = wp.perfect_hit[d];
            RefEnd = Hitcan._end;
            RefBegin = Hitcan._begin;

            // candidate alignment length 
            len1 = RefEnd - RefBegin;
            _m = ref.bfa[Hitcan.chr].s;

            // get iseq of ref 
            j = 0;
            for (i=RefBegin; i<RefEnd; i++)
            { 
                b = (i%12 + 1)*2;
                // seed_bits = 3
                seqjseg[j] = b<=24 ? (_m[i/12].a>>(24-b))&3 : (_m[i/12].a<<(b-24)|_m[i/12+1].a>>48-b)&3;
                j++;
            }
            // debug only
#ifdef DEBUG
            cerr << "len1 " << len1 << " RefBegin " << RefBegin << " RefEnd " << RefEnd << endl;
#endif
            diag_test_aapn_est_circular(seqjseg, len, len1, best_sum, band_left, band_right, wp);
#ifdef DEBUG
            cerr << "band_left " << band_left << " band_right " << band_right << endl;
#endif
            if (best_sum < wp.best_kmers) continue;

            Gramunit.chr = Hitcan.chr;
            Gramunit._begin = RefBegin;
            Gramunit._end = RefEnd;
            Gramunit.qgrams = best_sum;

            // location of alignment begin
            Gramunit.bandleft = band_left;  

            //location of alignment end
            Gramunit.bandright = band_right;
            Gramsort.push_back(Gramunit);
        }

        // release candidate hits vector memory
        wp.perfect_hit.resize(0);
        sort(Gramsort.begin(),Gramsort.end());
        trys = 0;//try DPs
        repreports = 0;//repeats reported

        //for dp alignment
        for (d = 0; d< Gramsort.size(); d++)
        {
            Gramunit = Gramsort[d];
            RefEnd = Gramunit._end;
            RefBegin = Gramunit._begin;

            //candidate alignment length 
            len1 = RefEnd - RefBegin;
            _m = ref.bfa[Gramunit.chr].s;

            //get iseq of ref 
            j = 0;
            for (i=RefBegin; i<RefEnd; i++)
            { 
                b = (i%12 + 1)*2;
                seqjseg[j] = b<=24? (_m[i/12].a>>(24-b))&3 : (_m[i/12].a<<(b-24)|_m[i/12+1].a>>48-b)&3;
                j++;
            }

            // band alignment function
            local_band_align2(wp.iseqquery, seqjseg, len, len1, best_score, Gramunit.bandleft, Gramunit.bandright, talign_info, tiden_no, alnln, wp);
            read_cov = abs(talign_info[2] - talign_info[1]) + 1;

            if (param.global_signal == 1)
            {
                read_identity = (tiden_no * 100)/_pread->length;
                read_identity_d = (double)(tiden_no * 100)/_pread->length;
            }
            else
            {
                if (read_cov < wp.align_len)
                { 
                    trys++;
                    if (trys>param.maxtrys) break;
                    continue;
                }

                read_identity = (tiden_no * 100)/alnln;
                read_identity_d = (double)(tiden_no * 100)/alnln;
            }

            if (read_identity < param.identity)
            {
                trys++;
                if (trys > param.maxtrys) break;
                continue;
            }

            // get e-value 
            eValue = rawScoreToExpect(best_score, mink, lamda, eLengthSeq, eLengthDB);
            if (eValue > param.evalue)
            { 
                continue;
            } 
            trys = 0;
            _RecruitedSignal = 1;

            // be recruited
            repreports++;

            // write alignment output to fout
            if (param.report_repeat_hits == 0)
            {
                if (param.outputformat == 1)
                {
                    //1:PSL fromat
                    sprintf(_ch, "%d\t%d\t0\t0\t0\t0\t0\t0\t+\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t1\t%d,\t%d,\t%d,\n",
                            tiden_no, read_cov-tiden_no, _pread->name.c_str(), _pread->length, talign_info[1],\
                            talign_info[2]+1, ref.title[Gramunit.chr].name.c_str(), ref.title[Gramunit.chr].size,\
                            RefBegin+talign_info[3], RefBegin+talign_info[4]+1, read_cov,talign_info[1],\
                            RefBegin+talign_info[3]);
                }
                else
                {
                    //0:FR-HIT format
                    sprintf(_ch, "%s\t%dnt\t%.1e\t%d\t%d\t%d\t+\t%0.2f%\t%s\t%d\t%d\n",
                            _pread->name.c_str(), _pread->length, eValue,read_cov,talign_info[1]+1,\
                            talign_info[2]+1, read_identity_d, ref.title[Gramunit.chr].name.c_str(), \
                            RefBegin+talign_info[3]+1, RefBegin+talign_info[4]+1);
                }
#pragma omp critical
                os.append(_ch);
            }
            else
            {
                // for repeats report
                if (repreports > param.report_repeat_hits) break;
                else
                {
                    Outputunit.eva=eValue;
                    if (param.outputformat == 1) 
                    {
                        sprintf(Outputunit.och, "%d\t%d\t0\t0\t0\t0\t0\t0\t+\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t1\t%d,\t%d,\t%d,\n",
                                tiden_no,read_cov-tiden_no,_pread->name.c_str(),_pread->length,talign_info[1],talign_info[2]+1,\
                                ref.title[Gramunit.chr].name.c_str(), ref.title[Gramunit.chr].size,RefBegin+talign_info[3],\
                                RefBegin+talign_info[4]+1, read_cov, talign_info[1], RefBegin+talign_info[3]);
                    }
                    else
                    {
                        sprintf(Outputunit.och, "%s\t%dnt\t%.1e\t%d\t%d\t%d\t+\t%0.2f%\t%s\t%d\t%d\n", \
                                _pread->name.c_str(),_pread->length,eValue,read_cov,talign_info[1]+1,talign_info[2]+1,\
                                read_identity_d,ref.title[Gramunit.chr].name.c_str(),RefBegin+talign_info[3]+1,\
                                RefBegin+talign_info[4]+1); 
                    }

                    Outputlist.push_back(Outputunit);

                }
            }

        }// end dp alignment

        Gramsort.resize(0);
    }
    // Random recruited finished
    //if ((param.report_repeat_hits == 0)&&(_RecruitedSignal == 1)) return 1;
    //complementary chain | done
    if ((0 == param.chains) || (2 == param.chains))
    {
        AhitCollect(_pread->length, seedstep, ref, wp.cseeds, wp.Ahit);

        // get alignment candidates
	if (param.identity >= 95){
            if (wp.Ahit.size() >= wp.bestPigeon){
                sort(wp.Ahit.begin(), wp.Ahit.end());
                tcc = wp.Ahit[0];
                HitcansCollectPigeon(tcc, ref, _pread->length, wp);
            }
        }else {
            if (wp.Ahit.size() > 1)
            {
            	sort(wp.Ahit.begin(),wp.Ahit.end());//sort Ahit
            	tcc = wp.Ahit[0];
            	HitcansCollect(tcc, ref, _pread->length, wp);
            }// end get alignment candidates
	}

        wp.Ahit.resize(0);//release initial seeds hits vector memory
        MergeAdjoiningBlock(wp);// Merge adjoining blocks
        wp._hit_Cans.resize(0);
        // Get 4-mers value of query
        CreateFourmers(wp.ciseqquery, _pread->length, wp);

        // analysis hits for 4-kmers count filter and alignment 
        for (d = 0; d< wp.perfect_hit.size(); d++)
        {
            Hitcan = wp.perfect_hit[d];
            RefEnd = Hitcan._end;
            RefBegin = Hitcan._begin;
            len1 = RefEnd - RefBegin; // candidate alignment length
            _m = ref.bfa[Hitcan.chr].s; // get reference

            // get iseq of ref 
            j = 0;
            for (i=RefBegin; i<RefEnd; i++)
            {
                b = (i%12 + 1)*2;
                //seed_bits
                seqjseg[j] = b<=24? (_m[i/12].a>>(24-b))&3 : (_m[i/12].a<<(b-24)|_m[i/12+1].a>>48-b)&3;
                j++;
            }

            diag_test_aapn_est_circular(seqjseg, len, len1, best_sum, band_left, band_right, wp);

            // filtering using 4-mers 
            if (best_sum < wp.best_kmers) continue;

            Gramunit.chr = Hitcan.chr;
            Gramunit._begin = RefBegin;
            Gramunit._end = RefEnd;
            Gramunit.qgrams = best_sum;
            // location of alignment begin
            Gramunit.bandleft = band_left;
            // location of alignment end
            Gramunit.bandright = band_right; 
            Gramsort.push_back(Gramunit);

        }
        wp.perfect_hit.resize(0);
        sort(Gramsort.begin(),Gramsort.end());
        trys = 0; //try DPs
        repreports = 0; //repeats reported

        //for dp alignment
        for (d = 0; d< Gramsort.size(); d++)
        {
            Gramunit = Gramsort[d];
            RefEnd = Gramunit._end;
            RefBegin = Gramunit._begin;
            // candidate alignment length 
            len1 = RefEnd - RefBegin;
            _m = ref.bfa[Gramunit.chr].s;
            // get iseq of ref 
            j = 0;
            for (i=RefBegin; i<RefEnd; i++)
            { 
                b = (i%12 + 1)*2;
                //seed_bits
                seqjseg[j] = b<=24? (_m[i/12].a>>(24-b))& 3: (_m[i/12].a<<(b-24)|_m[i/12+1].a>>48-b)&3;
                j++;
            }

            // band DP alignment 
            local_band_align2(wp.ciseqquery,seqjseg,len,len1,best_score,Gramunit.bandleft,Gramunit.bandright,talign_info,tiden_no,alnln,wp);

            // the alignment length of read
            read_cov = abs(talign_info[2] - talign_info[1]) + 1;

            if (param.global_signal == 1)
            {
                read_identity = (tiden_no * 100)/_pread->length;
                read_identity_d = (double)(tiden_no * 100)/_pread->length;
            }
            else
            {
                if (read_cov < wp.align_len)
                { 
                    trys++;
                    if (trys>param.maxtrys) break;
                    continue;
                }

                read_identity = (tiden_no * 100)/alnln;
                read_identity_d = (double)(tiden_no * 100)/alnln;
            }

            if (read_identity < param.identity)
            {
                trys++;
                if (trys>param.maxtrys) break;
                continue;
            }

            // get evalue
            eValue = rawScoreToExpect(best_score,mink,lamda,eLengthSeq,eLengthDB);
            if (eValue > param.evalue) continue;

            trys = 0;
            // be recruited
            _RecruitedSignal = 1; 
            repreports++;
            // write alignment output to fout
            if (param.report_repeat_hits == 0)
            {
                if (param.outputformat == 1)
                {
                    //1:PSL fromat
                    sprintf(_ch, "%d\t%d\t0\t0\t0\t0\t0\t0\t-\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t1\t%d,\t%d,\t%d,\n", 
                            tiden_no,read_cov-tiden_no,_pread->name.c_str(),_pread->length,_pread->length-talign_info[2]-1,\
                            _pread->length-talign_info[1],ref.title[Gramunit.chr].name.c_str(),ref.title[Gramunit.chr].size,\
                            RefBegin+talign_info[3],RefBegin+talign_info[4]+1,read_cov,talign_info[1],\
                            RefBegin+talign_info[3]);
                }
                else
                {
                    //0:FR-HIT format
                    sprintf(_ch, "%s\t%dnt\t%.1e\t%d\t%d\t%d\t-\t%0.2f%\t%s\t%d\t%d\n", _pread->name.c_str(),\
                            _pread->length,eValue,read_cov,_pread->length-talign_info[1],_pread->length-talign_info[2],\
                            read_identity_d, ref.title[Gramunit.chr].name.c_str(),RefBegin+talign_info[3]+1,\
                            RefBegin+talign_info[4]+1);
                }
#pragma omp critical
                os.append(_ch);

            }
            else
            {
                // for repeats report, only random one
                if (repreports > param.report_repeat_hits) break;
                else
                {
                    Outputunit.eva=eValue;
                    if (param.outputformat == 1) 
                    {
                        sprintf(Outputunit.och, "%d\t%d\t0\t0\t0\t0\t0\t0\t-\t%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t1\t%d,\t%d,\t%d,\n",
                                tiden_no,read_cov-tiden_no,_pread->name.c_str(),_pread->length, _pread->length-talign_info[2]-1,\
                                _pread->length-talign_info[1],ref.title[Gramunit.chr].name.c_str(),ref.title[Gramunit.chr].size,\
                                RefBegin+talign_info[3],RefBegin+talign_info[4]+1,read_cov,talign_info[1],RefBegin+talign_info[3]);
                    }
                    else
                    {
                        sprintf(Outputunit.och, "%s\t%dnt\t%.1e\t%d\t%d\t%d\t-\t%0.2f%\t%s\t%d\t%d\n", _pread->name.c_str(),\
                                _pread->length,eValue,read_cov,_pread->length-talign_info[1],_pread->length-talign_info[2],\
                                read_identity_d,ref.title[Gramunit.chr].name.c_str(),RefBegin+talign_info[3]+1,\
                                RefBegin+talign_info[4]+1);
                    }
                    Outputlist.push_back(Outputunit);
                }

            } //end if 
        } // end 4-mers filtering and alignment

        Gramsort.resize(0);//release candidate hits vector memory

    } //end complementary chain processing

    //output N best top hits
    if (param.report_repeat_hits != 0)
        if (Outputlist.size()>0) 
        {
            sort(Outputlist.begin(),Outputlist.end());
            j = Outputlist.size();
            if (j>param.report_repeat_hits) j = param.report_repeat_hits;
#pragma omp critical
            {
                for (i=0; i<j; i++)
                    os.append(Outputlist[i].och);
            }
            Outputlist.resize(0);
        }
    
    return _RecruitedSignal;

}

//filtering HSP fasta//len1:reads length, len2:ref seq segment len, iseq2:ref seq segment binary
int ReadAlign::diag_test_aapn_est_circular(char iseq2[],int len1,int len2, int &best_sum,int &band_left, int &band_right,WorkingPara &wp) 
{
    int i, j, k;
    int nall = len1 + len2 - 1;
    int *pp = wp.diag_score;

    for (i=nall; i; i--) *pp++=0;

    int len22 = len2 - 3;
    int i1 = len1 - 1;

    for (i=0; i<len22; i++, i1++, iseq2++)
    {
        int c22 = iseq2[0]*NAA3 + iseq2[1]*NAA2 + iseq2[2]*NAA1 + iseq2[3];

        if ((j = wp.taap[c22]) == 0) continue;

        pp = wp.aap_list+ wp.aap_begin[c22];

        for (; j; j--, pp++)
        {
            wp.diag_score[i1 - *pp]++;
        }

    }

    // find the best band range
    int required_aa1 = wp.best_nas;
    int band_b = required_aa1 >= 1 ? required_aa1-1:0;
    int band_e = nall - required_aa1;
    int band_m = ( band_b+band_width-1 < band_e ) ? band_b+band_width-1 : band_e;
    int best_score = 0;

    for (i=band_b; i<=band_m; i++)
    {
        best_score += wp.diag_score[i];
    }

    int from = band_b;
    int end = band_m;
    int score = best_score;

    for (k=from, j=band_m+1; j<band_e; j++) 
    {
        score -= wp.diag_score[k++];
        score += wp.diag_score[j];

        if (score > best_score)
        {
            from = k;
            end  = j;
            best_score = score;
        }
    }

    for (j=from; j<=end; j++) 
    {
        if ( wp.diag_score[j] < 5 ) 
        { 
            best_score -= wp.diag_score[j]; 
            from++;
        }else break;
    }

    for (j=end; j>=from; j--) 
    { 
        if ( wp.diag_score[j] < 5 ) 
        { 
            best_score -= wp.diag_score[j]; 
            end--;
        } else break;
    }

    band_left = from - len1 + 1;
    band_right = end - len1 + 1;

    best_sum = best_score;

    return 1;
}

int ReadAlign::local_band_align2(char iseq1[], char iseq2[], int len1, int len2, int &best_score, int band_left, int band_right, int *talign_info, int &iden_no, int &alnln, WorkingPara &wp)
{

    //iseq1: binary read sequence, len1: reads length
    //iseq2: ref seq segment, len2: ref seq length
    int **score_mat(wp.score_mat);
    int **iden_mat(wp.iden_mat);
    int **from1_mat(wp.from1_mat);
    int **from2_mat(wp.from2_mat);
    int **alnln_mat(wp.alnln_mat);
    int *gap_array(mat.gap_array);
    
    int &from1(talign_info[1]);
    int &from2(talign_info[3]);

    int &end1(talign_info[2]);
    int &end2(talign_info[4]);
    
    int i, j, k, j1;
    int jj, kk;
    int best_score1, iden_no1;
    int best_from1, best_from2, best_alnln;

    iden_no = 0;

    from1=0;
    from2=0;

    if ((band_right >= len2) || (band_left <= -len1) || (band_left > band_right)) return 0;
    
    int bandwidth = band_right - band_left + 1;
    
    for (i=0; i<len1; i++) 
        for (j1=0; j1<bandwidth; j1++) //here index j1 refer to band column
            score_mat[i][j1] =  0;
    
    best_score = 0;
    
    if (band_left < 0) 
    {
        //set score to left border of the matrix within band
        int tband = (band_right < 0) ? band_right : 0;

        for (k=band_left; k<=tband; k++) 
        { 
            i = -k;
            j1 = k - band_left;

            if (( score_mat[i][j1] = mat.matrix[iseq1[i]][iseq2[0]] ) > best_score) 
            {
                best_score = score_mat[i][j1];
                from1 = i; from2 = 0; end1 = i; end2 = 0; alnln = 1;
            }

            iden_mat[i][j1] = (iseq1[i] == iseq2[0]) ? 1 : 0;
            from1_mat[i][j1] = i;
            from2_mat[i][j1] = 0;
            alnln_mat[i][j1] = 1;
        }
    }

    if (band_right >=0)
    { //set score to top border of the matrix within band

        int tband = (band_left > 0) ? band_left : 0;

        for (i=0,j=tband; j<=band_right; j++) 
        {
            j1 = j - band_left;
            if (( score_mat[i][j1] = mat.matrix[iseq1[i]][iseq2[j]] ) > best_score) 
            {
                best_score = score_mat[i][j1];
                from1 = i; from2 = j; end1 = i; end2 = j; alnln = 0;
            }

            iden_mat[i][j1] = (iseq1[i] == iseq2[j]) ? 1 : 0;
            from1_mat[i][j1] = i;
            from2_mat[i][j1] = j;
            alnln_mat[i][j1] = 1;
        }
    }
    
    for (i=1; i<len1; i++) 
    {
        for (j1=0; j1<bandwidth; j1++) 
        {
            j = j1 + i + band_left;
            if (j<1 || j>=len2) continue;

            // from (i-1,j-1)
            if ((best_score1 = score_mat[i-1][j1] )> 0) 
            {
                iden_no1 = iden_mat[i-1][j1];

                best_from1 = from1_mat[i-1][j1];
                best_from2 = from2_mat[i-1][j1];

                best_alnln = alnln_mat[i-1][j1] + 1;
            }
            else
            {
                best_score1 = 0;
                iden_no1 = 0;

                best_from1 = i;
                best_from2 = j;

                best_alnln = 1;
            }
            // from last row
            int s1, k0;
            k0 = (-band_left+1-i > 0) ? -band_left+1-i : 0;

            for (k=j1-1, kk=0; k>=k0; k--, kk++) 
            {
                if ((s1 = score_mat[i-1][k] + gap_array[kk] ) > best_score1) 
                {
                    best_score1 = s1;
                    iden_no1 = iden_mat[i-1][k];
                    best_from1 = from1_mat[i-1][k];
                    best_from2 = from2_mat[i-1][k];
                    best_alnln = alnln_mat[i-1][k]+kk+2;
                }
            }
            
            k0 = (j-band_right-1 > 0) ? j-band_right-1 : 0;

            for (k=i-2, jj=j1+1,kk=0; k>=k0; k--,kk++,jj++) 
            {
                if ((s1 = score_mat[k][jj] + gap_array[kk] ) > best_score1)
                {
                    best_score1 = s1;
                    iden_no1 = iden_mat[k][jj];
                    best_from1 = from1_mat[k][jj];
                    best_from2 = from2_mat[k][jj];
                    best_alnln = alnln_mat[k][jj]+kk+2;
                }
            }

            best_score1 += mat.matrix[iseq1[i]][iseq2[j]];

            if (iseq1[i] == iseq2[j]) iden_no1++;

            score_mat[i][j1] = best_score1;
            iden_mat[i][j1]  = iden_no1;
            from1_mat[i][j1] = best_from1;
            from2_mat[i][j1] = best_from2;
            alnln_mat[i][j1] = best_alnln;

            if (best_score1 > best_score)
            {
                best_score = best_score1;
                iden_no = iden_no1;

                end1 = i; 
                end2 = j;

                from1 = best_from1; 
                from2 = best_from2; 

                alnln = best_alnln;
            }
        } //END for j1
    } //END for (i=1; i<len1; i++)

    //printscorematrix(len1,bandwidth);

    return 1;

} //END int local_band_align2

void ReadAlign::printscorematrix(int **sm, int xlen, int ylen) 
{
    for (int i=0; i<xlen; i++) 
    {
        for (int j=0; j<ylen; j++)
            cerr<<sm[i][j]<<' ';
        cerr<<endl;
    }
    cerr<<endl;

}

