
/*
 * align.h for FR-HIT
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

#ifndef _ALIGNPROCESS_H_
#define _ALIGNPROCESS_H_

#include<vector>
#include<string>
#include<cstdio>
#include<bitset>
#include<cmath>
#include<algorithm>

#include "param.h"
#include "reads.h"
#include "refseq.h"
#include "utilities.h"

//#define DEBUG

using namespace std;

// For e-value | Karlin-Altschul statistics
/*
   1,                // A
   -2, 1,            // C
   -2,-2, 1,         // G
   -2,-2,-2, 1,      // T
   -2,-2,-2, 1, 1,   // U
   -2,-2,-2,-2,-2, 1 // N
   //A  C  G  T  U  N

fixed setting:

match score		1;
unmatch score	-2;
Gap open penalties 6;
Gap extend penalies 1;

*/

const double mink  = 0.621; // minor constant | K
const double lamda = 1.33; // Lambda parameter
const double Hh    = 1.12; // nats/aligned pairs

// For bseq array
const int MAXSNPS = 5;
const int MAX_SEQ = 2000;
const int MAX_AA  = 23;
const int MAX_GAP = 4096; //65536;
const int MAX_NA  = 6;
const int MAX_DIAG    = 65535;
const int MAX_THREADS = 128;
const int MAX_READ    = 3000;

// Ref part sampled  
// Maximum alignment length part on reference
const int _MaxPartLen = 2*MAX_READ + 12*3;
const int MAX_UAA = 4;
const int NAA1= MAX_UAA;
const int NAA2= MAX_UAA*MAX_UAA;
const int NAA3= NAA2*MAX_UAA;
const int NAA4= NAA2*NAA2;

extern Param param;
extern char rev_char[];

// Scoring Matrix 
class AA_MATRIX
{ 
    public:
        AA_MATRIX(){set_to_na();};
        
        int gap;
        int ext_gap;
        int gap_array[MAX_GAP];
        int matrix[MAX_AA][MAX_AA];
        
        void init();

        void set_gap(int gap1, int ext_gap1);
   	void set_matrix(int *mat1);
   	void set_to_na();

}; // END class AA_MATRIX

// Qgram hit location
struct Hit
{

    Hit()
    {
        chr = 0;
        loc = 0;
    }

    // Index of ref
    ref_id_t chr;

    // 0 based location of first bp on reference seq
    ref_loc_t loc;

};

// Candidate alignment location
struct Hit_Can
{

    Hit_Can()
    {
        chr = 0;
        _end = 0;
        _begin = 0;
    }

    // Index of ref
    ref_id_t chr;

    // Location of alignment end
    ref_loc_t _end; 

    // Location of alignment begin
    ref_loc_t _begin;   

};

// 4-mers sorting
struct MerSort
{

    MerSort()
    {
        chr = 0;
        _begin = 0;
        _end = 0;
        qgrams = 0;
        bandleft = 0;
        bandright = 0;
    } 
    
    ref_id_t chr;
    ref_loc_t _end;
    ref_loc_t _begin;

    int qgrams;

    // Location of alignment begin
    int bandleft;

    // Location of alignment end;
    int bandright;
    
    bool operator < (const MerSort& rhs) const { return qgrams > rhs.qgrams; }
};

// E-value sorting
struct OutputSort
{
    
    OutputSort()
    {
        eva = 0.0;
    } 

    // E-value
    double eva;  

    // Result output container
    char och[1000];
    bool operator < (const OutputSort& rhs) const { return eva < rhs.eva; }
};

class WorkingPara
{
    
    protected:

        bit32_t seeds[MAX_READ];   //seeds or kmers for a read
        bit32_t cseeds[MAX_READ];  //reverse complementary kmers of a read

        // For reads info  
        // Binary seq, size is: MAX_READ/12+1
        // and MAX_READ=3000
        bit24_t bseq[MAX_READ/12+1];
        // Size is: MAX_READ/12+1, 
        bit24_t cbseq[MAX_READ/12+1];
        
        // variables for DP aliggnment
        // NAA4=256, subscript is 4-mers hash and value is count
        int taap[NAA4];
        // NAA4=256, begin position in aap_list 
        int aap_begin[NAA4];
        // 65536, position info of a read stored in aap_list, 
        // indexed by aap_begin
        int aap_list[65535];    
        
        // Band estimation
        int diag_score[MAX_DIAG];  //[MAX_DIAG=65535] band estimation
        // All alignment hits
        // high 32 bits is refseq id, 
        // low 32 bits is kmer position in ref - kmer position of read
        vector<bit64_t> Ahit;     
        // Hits for DP alignment
        // there are at least 2 hits within these ranges?
        vector<Hit_Can> _hit_Cans; 
        vector<Hit_Can> perfect_hit; //hits with two kmers match
        
        // iseq for filter and alignment (read)
        char iseqquery[MAX_READ+12];     //char[MAX_READ+12]
        char ciseqquery[MAX_READ+12];    //char[MAX_READ+12]
        
        // Memory for DP alignment variables
        int *score_mat[_MaxPartLen];      //[_MaxPartLen] * [_MaxPartLen] DP alignment space
        int *iden_mat[_MaxPartLen];       //[_MaxPartLen] * [_MaxPartLen]
        int *from1_mat[_MaxPartLen];      //[_MaxPartLen] * [_MaxPartLen]
        int *from2_mat[_MaxPartLen];      //[_MaxPartLen] * [_MaxPartLen]
        int *alnln_mat[_MaxPartLen];      //[_MaxPartLen] * [_MaxPartLen]

        int align_len;       //-m default 30 when -g=0
        int best_nas;        //default 24, bps needed
        int best_kmers;      //default 20, 4-mers
	int bestPigeon;
        
        WorkingPara(){init();}
        ~WorkingPara(){final();}

        void init();
        void final();
        // Maybe better for each thread to have one
        // (conclusion: no different, so return to one in the class)
        // AA_MATRIX scorematrix;       

	//DP alignment
        friend class ReadAlign;

};

// Recruitment
class ReadAlign
{
    public:

        ReadAlign();
        ~ReadAlign(){};
        
        bit32_t num_reads;
        vector<ReadInf> *pmreads;

        void ImportBatchReads(bit32_t n, vector<ReadInf> *a)
        { 
            num_reads = n; 
            pmreads = a;
        }

	bit32_t n_aligned;
	AA_MATRIX mat; //DP alignment
	int band_width; 

        // Alignment results, 
        // prepare for output
        string _str_align;
        string str_align[MAX_THREADS];

        double rawScoreToExpect(int raw, double k, double lambda, int m, bit64_t n)
        {
            return k*m*n*exp(-1*lambda*raw);
        }

        int expectedHSPlength(double k, int m, bit64_t n, double h)
        {
            return int(log(k*m*n)/h);
        }

        // Effective length query//e-value
        int effectiveLengthSeq(int m, int hsp, double k)
        {
            return (m - hsp) < 1/k ? int(1/k) : m - hsp;
        }

        // Effective length of database
        bit64_t effectiveLengthDb(bit64_t n, int hsp, int seqnum, double k)
        {
            int n_prime = n - (seqnum*hsp);  return n_prime < 1/k ? int(1/k) : n_prime;
        }

        void Do_Batch(RefSeq &ref, ReadClass &read_a, ifstream &fin_a, ofstream &fout);
        void GenerateSeeds(int _read_length, WorkingPara &wp);

        //void Do_Batch(RefSeq &ref);
        int ConvertBinaySeq(vector<ReadInf>::iterator &_pread, WorkingPara &wp);
        int AlignProcess(RefSeq &ref, vector<ReadInf>::iterator &_pread, string &os, WorkingPara &wp);
        int diag_test_aapn_est_circular(char iseq2[], int len1, int len2, int &best_sum, int &band_left, int &band_right, WorkingPara &wp);
        int local_band_align2(char iseq1[], char iseq2[], int len1, int len2, int &best_score, int band_left, int band_right, int *talign_info, int &iden_no, int &alnln, WorkingPara &wp);

        void AhitCollect(int _read_length, int seedstep, RefSeq &ref, bit32_t *seeds, vector<bit64_t> &Ahit);
        // Extract info from Ahit to _hit_Cans
        void HitcansCollect(bit64_t tcc, RefSeq &ref, int _read_length, WorkingPara &wp);
	void HitcansCollectPigeon(bit64_t tcc, RefSeq &ref, int _read_length, WorkingPara &wp);
        // Operate on _hit_Cans and perfect_hit
        void MergeAdjoiningBlock(WorkingPara &wp);
        // Deal with taap, iseqquery, aap_begin, aap_list
        void CreateFourmers(char *iseqquery, int _read_length, WorkingPara &wp);
public:
        int _format; //fasta or fastq, seems useless for now

        void ImportFileFormat(int format)
        {
            _format = format;
        }

        void SetFlag(char c)
        {
            _setname = c;
        }

        char _setname;
private:
        // For debug
        void printscorematrix(int **sm, int xlen, int ylen);
};

#endif //_ALIGNPROCESS_H_
