package uni.bielefeld.cmg.sparkhit.util;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Created by Liren Huang on 13/01/16.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */
public class DefaultParam implements Serializable{

    public DefaultParam (){
        /**
         * This is a class of data structure which stores the default parameters
         */
    }

    public String mightyName = "SparkHit";
    public String inputBuildPath;  // build reference data index, used by builder

    public String inputResultPath; // report recruitment statistics, used by reporter
    public int outputformat = 0; // used by converter
    public String inputFqPath; // input query file, used by main program and converter

    public String inputFqLinePath;  // input query file, one line per unit
    public String inputFaPath; // input reference file, used by main program

    public String toolDepend = " "; // external tool dependencies such as sh, perl, java package
    public String tool;  // external tool such as bwa mem, bowtie2
    public String toolParam = " ";  // external tool parameter for your tool
    public boolean filterFastq = false;
    public boolean filterToFasta = false;
    public boolean lineToFasta = false;

    public int window=10000;

    public String inputList;
    public int inputNodes;

    public String outputPath;  // used by all

    public int threads = 1;
    public int partitions = 0;

    public  int kmerSize = 11;              // default kmer length
    public  int kmerBits = (1 << (kmerSize*2)) - 1; //binary length of kmer (2bits for one Nt, all positions are one. Eg. A=3, 11111111 AAAA)
    public  int maximumKmerNum = 1<<(kmerSize*2);   // how many kmers at maximum
    public  int kmerOverlap = 8;            // skip kmerSize - kmerOverlap every extention
    public  int bandWidth = 4;              // band width for banded alignment

    public  int minReadSize=20;             // minimum read length for alignment
    public  int maxReadSize = 500;         // maximum read length for alignment
    public  int readIdentity = 75;          // minimum identity to report a hit
    public  int globalOrLocal = 1;          // globle or local alignment, 0 for local
    public  int alignLength = 30;           // minimal alignment coverage control for the read (local alignment)
    public  int reportRepeatHits = 0;       // whether report repeat hits or not
    public  int maskRepeat = 1;             // whether mask repeat or not
    public  Pattern validNt = Pattern.compile("[ACGT]");            // good Nucleotides
    public  Pattern invalidNt = Pattern.compile("[NXacgt]");        // invalid Nucleotides
    public  Pattern validNtNomask = Pattern.compile("[ACGTacgt]");  // for some genomes lower case char are not repeat areas
    public  Pattern nxNomask = Pattern.compile("[NX]");             // only mask NX
    public  int bestKmers = 20;             // 4-mers q-gram filter part, Number of minimum qGrams
    public  int bestNas = 24;               // bps 80% correspond to 30nt of alignLength
    public  int bestPigeon = 2;             // at least 2 kmers to form a block
    public  int maxTrys = 20;               // tries for alignment
    public  int skipThreshold = 1000;       // threshold for long reads with 2bp skip per extension
    public  int globalSignal = 0;           //

    public  double eValue = 10d;                     // default evalue cutoff
    public  int chains = 0;                 // alignment for chains: 1 positive; 2 complementary; 0 Both
    public  char[] flagChains = {'+', '-'};
    public  char[] codeNt = {'A','C','G','T'};
    public  char[] codeNtComplement = {'T', 'G', 'C', 'A'};

    public int[] alphaCode = initialAlphaCode();
    public int[] alphaCodeComplement = initialAlphaCodeComplement();
    public int[] alphaCodeNNNNN = initialNNNNNFilter();

    public double minor = 0.621;
    public double lambda = 1.33;
    public double pairAlign = 1.12;

    public int[] initialAlphaCode(){
        int[] alphaCodeInitial = new int[256];
        for (int i=0; i<256; i++){
            alphaCodeInitial[i] = 0;         // 'a' 'A' ASCII code and all other Char
        }

        alphaCodeInitial['c']=alphaCodeInitial['C']=1; // 'c' is ASCII number of c
        alphaCodeInitial['g']=alphaCodeInitial['G']=2; // the same
        alphaCodeInitial['t']=alphaCodeInitial['T']=3; // the same

        return alphaCodeInitial;
    }

    public int[] initialAlphaCodeComplement(){
        int[] alphaCodeComplementInitial = new int[256];
        for (int i=0; i<256; i++){
            alphaCodeComplementInitial[i] = 3;
        }

        alphaCodeComplementInitial['c']=alphaCodeComplementInitial['C']=2;
        alphaCodeComplementInitial['g']=alphaCodeComplementInitial['G']=1;
        alphaCodeComplementInitial['t']=alphaCodeComplementInitial['T']=0;
        return alphaCodeComplementInitial;
    }

    public int[] initialNNNNNFilter(){
        int[] alphaCodeNNNNNInitial = new int[256];
        for (int i=0; i<256; i++){
            alphaCodeNNNNNInitial[i] = 1;
        }
        alphaCodeNNNNNInitial['c']=alphaCodeNNNNNInitial['C']=0;
        alphaCodeNNNNNInitial['g']=alphaCodeNNNNNInitial['G']=0;
        alphaCodeNNNNNInitial['t']=alphaCodeNNNNNInitial['T']=0;
        alphaCodeNNNNNInitial['a']=alphaCodeNNNNNInitial['A']=0;
        return alphaCodeNNNNNInitial;
    }

    /* change kmer length and maximum bit */
    public  void setKmerSize(int k){
        kmerSize = k;
        kmerBits = (1 << (kmerSize*2))-1;
        maximumKmerNum = 1<<(kmerSize*2); // re-initial maximumKmerNum according to kmerSize
    }

    public void setKmerOverlap(int o){
        kmerOverlap = o;
    }

}
