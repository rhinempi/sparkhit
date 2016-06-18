/*
 * main.cpp for FR-HIT
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

#include<unistd.h>
#include<cstdio>
#include<iostream>
#include<iomanip>
#include<fstream>
#include<string>
#include<vector>
#include<algorithm>
#include<omp.h>

#include "reads.h"
#include "refseq.h"
#include "align.h"
#include "param.h"
#include "utilities.h"

using namespace std;

// Global variables
Param param;
string ref_file; // Reference file
string query_a_file; // Query reads file
string query_q_file; // Query reads fastq file
string out_align_file; // Output algnment file

ifstream fin_a;
ifstream fin_db;
ofstream fout;

RefSeq ref;
ReadClass read_a;

// Recruitment procedure
unsigned int Do_ReadAlign()
{
//    read_a.CheckFile(fin_a);
    ReadAlign a;
    a.ImportFileFormat(read_a._file_format);
    a.SetFlag('a');
    a.Do_Batch(ref,read_a,fin_a,fout);
    return a.n_aligned;
}

// Usage
void usage(void)
{
    cerr<<"\nUsage:   fr-hit v0.7 [options]\n"

	<<"\n\tThis program has been modified with Pigeon hole filtering\n\n"

        <<"       -a   <string>   reads file, *.fasta format\n"
	<<"       -q   <string>   reads file, *.fastq format\n"
        <<"       -d   <string>   reference genome sequences file, *.fasta format\n"
        <<"       -o   <string>   output recruitments file\n"
        <<"       -e   <double>   e-value cutoff, default="<<param.evalue<<"\n"
        <<"       -u   <int>      mask out repeats as lower cased sequence to prevent spurious hits? 1: yes; 0: no; default="<<param.mask<<"\n"
        <<"       -f   <int>      format control for output file,0:FR-HIT format; 1:PSL fromat, default="<<param.outputformat<<"\n"
        <<"       -k   <int>      k-mer size (8<=k<=12), default="<<param.seed_size<<"\n"
        <<"       -p   <int>      k-mer overlap of index (1<=p<-k), using small overlap for longer reads(454, Sanger), default="<<param.seed_overlap<<"\n"
        <<"       -c   <int>      sequence identity threshold(%), default="<<param.identity<<"\n"
        <<"       -g   <int>      use global or local alignment? 1:global; 0:local (need -m), default="<<param.global_signal<<"\n"
        <<"       -w   <int>      minimal read length to use 2bp k-mer index step to 454 long reads, default="<<param.lenforstep<<"\n"
        <<"       -m   <int>      minimal alignment coverage control for the read (g=0), default="<<param.align_len<<"\n"
        <<"       -l   <int>      length of throw_away_reads, default="<<param.min_read_size<<"\n"
        <<"       -t   <int>      maximum number of failed alingment attempts, default="<<param.maxtrys<<"\n"
        <<"       -r   [0,N]      how to report alignment hits, 0:all; N:the best top N hits for one read, default="<<param.report_repeat_hits<<"\n"
        <<"       -n   <int>      do alignment for which chain? 0:both; 1:direct only; 2:complementary only. default="<<param.chains<<"\n"
        <<"       -b   <int>      band_width of alignment, default="<<param.band<<"\n"
        <<"       -T   [0,N]      number of threads, default 1; with 0, all CPUs will be used"<<endl
        <<"       -h   help\n\n"
        
        <<endl;
    
    exit(1);
};

int mGetOptions(int rgc, char *rgv[])
{
    // options
    int i;
    for (i=1; i<rgc; i++)
    {
        if (rgv[i][0]!='-') return i;
        switch(rgv[i][1]) 
        {
            case 'a': query_a_file = rgv[++i]; break;
	    case 'q': query_q_file = rgv[++i]; break;
            case 'd': ref_file = rgv[++i]; break;
            case 'k': param.SetSeedSize(atoi(rgv[++i])); break;
            case 'p': param.seed_overlap = atoi(rgv[++i]); break;
            case 'o': out_align_file = rgv[++i]; break;
            case 'u': param.mask=atoi(rgv[++i]); break;
            case 'g': param.global_signal = atoi(rgv[++i]); break;
            case 'f': param.outputformat = atoi(rgv[++i]); break;
            case 'l': param.min_read_size = atoi(rgv[++i]); break;
            case 't': param.maxtrys = atoi(rgv[++i]); break;
            case 'c': param.identity = atoi(rgv[++i]); break;
            case 'm': param.align_len = atoi(rgv[++i]); break;
            case 'w': param.lenforstep = atoi(rgv[++i]); break;
            case 'e': param.evalue = atof(rgv[++i]); break;
            case 'r': param.report_repeat_hits = atoi(rgv[++i]); break;
            case 'n': param.chains=atoi(rgv[++i]); break;
            case 'b': param.band=atoi(rgv[++i]); break;
            case 'T': param.ncpu=atoi(rgv[++i]); 
                      if (param.ncpu < 0) param.ncpu = 1;
                      if (param.ncpu > MAX_THREADS) param.ncpu = MAX_THREADS;
                      break;
            case 'h':usage(); // Usage information
            case '?':usage(); // Unrecognizable input
        }
    }

    // 4-mers filtering cutoff and 
    // this idea is from cd-hit
    // bps aligned correct
    param.best_nas = (param.align_len*param.identity)/100;
    // 4-mers
    param.best_kmers = param.align_len-(param.align_len-param.best_nas)*4 - 3;
    param.best_pigeon = param.align_len / param.seed_size -1 - (param.align_len - param.best_nas);
	if (param.best_pigeon < 1) param.best_pigeon = 1;

	if (param.identity >= 95 ){
		param.seed_overlap=0;
	}
    // One candidate matching block consists at least one seed (4-mers)
    if (param.best_kmers < (param.seed_size - 3))
    {
        param.best_kmers = param.seed_size - 3;
    }
    // Repeats mask or nomask 
    if (param.mask == 0)
    {
        param.useful_nt=param.useful_nt_nomask;
        param.nx_nt=param.nx_nt_nomask;
    }
    
    return i;
}

void RunProcess(void) {

    if (!query_a_file.empty())
    {
        fin_a.open(query_a_file.c_str());
        if (!fin_a)
        {
            cerr << "failed to open file: "<< query_a_file << endl;
            exit(1);
        }
    }else if (!query_q_file.empty()){
	fin_a.open(query_q_file.c_str());
	param.fin_q = true;
	if (!fin_a)
	{
	    cerr << "failed to open file: "<< query_q_file << endl;
	    exit(1);
	}
    }
    else
    {
        cerr <<"missing query file(s)\n";
        exit(1);
    }
    cerr << "Read recruitment:\n";
    cerr << "Query: " << query_a_file << "  Reference: " << ref_file << endl;
    fout.open(out_align_file.c_str());
    if (!fout)
    {
        cerr << "failed to open file: " << out_align_file << endl;
        exit(1);
    }

    unsigned int n_aligned(0); // Number of reads recruited
    read_a.InitialIndex();
    n_aligned=Do_ReadAlign();
    fin_a.close();
    fout.close();
    cerr << "Total number of reads recruited: " << n_aligned << " (" << setprecision(2) << 100.0*n_aligned/read_a._index << "%)\n";
    cerr << "Done.\n";
    cerr << "Finished at " << Curr_Time();
    cerr << "Total time consumed:  " << Cal_AllTime() << " secs\n";
}

int main(int argc, char *argv[])
{
    if (argc == 1) usage(); // Print usage

    for (int i=0; i<argc; i++) 
    {
        cerr << argv[i] << ' ';
        cerr << endl;
    }

    Initial_Time();
    cerr<<"Start at:  "<<Curr_Time()<<endl;
    int noptions = mGetOptions(argc, argv);
    // Mutltithreads part
#if defined (_OPENMP)
    if (param.ncpu) omp_set_num_threads(param.ncpu);
#endif
    fin_db.open(ref_file.c_str());

    if (!fin_db)
    {
        cerr << "fatal error: failed to open ref file\n";
        exit(1);
    }

    ref.Run_ConvertBinseq(fin_db);
    cerr << "Load in " << ref.total_num << " reference seqs, total size " << ref.sum_length << " bp. " << Cal_AllTime() << " secs passed" << endl;
    ref.CreateIndex(); // Qgram_Index();

    cerr << "Create refseq k-mer index table. " << Cal_AllTime() << " secs passed" << endl;

    RunProcess();
    
    return 0;
}

