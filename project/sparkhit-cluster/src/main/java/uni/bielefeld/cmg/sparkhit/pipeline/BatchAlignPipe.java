package uni.bielefeld.cmg.sparkhit.pipeline;


import uni.bielefeld.cmg.sparkhit.algorithm.Arithmetic;
import uni.bielefeld.cmg.sparkhit.io.readInfo;
import uni.bielefeld.cmg.sparkhit.matrix.ScoreMatrix;
import uni.bielefeld.cmg.sparkhit.reference.RefStructBuilder;
import uni.bielefeld.cmg.sparkhit.struct.*;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Liren Huang on 22/02/16.
 *
 *      spark-hit_standalone
 *
 * Copyright (c) 2015-2015
 *      Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 * 
 * spark-hit_standalone is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 *
 */


/**
 * Returns an object for recruiting one read to the reference genomes.
 * This is the main pipeline for fragment recruitment.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class BatchAlignPipe implements Serializable{
    private DefaultParam param;
    private AlignmentParameter pAlign;

    public List<BinaryBlock> BBList;
    public List<RefTitle> listTitle;
    public KmerLoc[] index;
    public ScoreMatrix mat;
    public long totalLength;
    public int totalNum;



    /**
     * A constructor that construct an object of {@link BatchAlignPipe} class.
     *
     * @param param the preset parameters.
     */
    public BatchAlignPipe(DefaultParam param){
        this.param = param;
        setAlignmentParameter();
    }

    /**
     *
     * A constructor that construct an object of {@link BatchAlignPipe} class.
     *
     * no parameter needed.
     */
    public BatchAlignPipe(){

    }

    /**
     * This method recruits one sequencing read to the reference genomes.
     *
     * @param read {@link readInfo}.
     * @return the recruitment result and the match info.
     */
    public String recruit (readInfo read){
        List<String> alignResult;
        String alignmentResult = "";

        ReadInfo rInfo = new ReadInfo(read);

        if (rInfo.readSize < param.minReadSize){
            return alignmentResult;
        }

        if (param.globalOrLocal == 1){
            pAlign.alignLength = rInfo.readSize;
            pAlign.bestNas = (pAlign.alignLength * param.readIdentity) / 100;
            pAlign.bestKmers = pAlign.alignLength - (pAlign.alignLength - pAlign.bestNas) * 4 - 3;
            if (param.readIdentity >= 94) {
                pAlign.bestPigeon = pAlign.alignLength / param.kmerSize - 1 - (pAlign.alignLength - pAlign.bestNas);
                if (pAlign.bestPigeon <=1 ) pAlign.bestPigeon = 2;
            }
        }

        if (readBinaryBlock(rInfo)!=0){
            return alignmentResult;
        }

        getKmers(rInfo.readSize);

        alignResult = alignment(rInfo);

        for (String s : alignResult)
        {
            alignmentResult += s + "\t";
        }

        return alignmentResult;
    }

    /**
     * This method recruits one sequencing read to the reference genomes.
     * Different from {@link BatchAlignPipe#recruit}, this method returns
     * a list of recruitment results.
     *
     * @param read a sequencing read in {@link String}.
     * @return a list of recruitment results and the match info.
     */
    public List<String> sparkRecruit (String read){
        List<String> alignmentResult = new ArrayList<String>();

        ReadInfo rInfo = new ReadInfo(read);

        if (rInfo.readSize < param.minReadSize){
            return alignmentResult;
        }

        if (rInfo.readSize > param.maxReadSize){
            return alignmentResult;
        }

        if (param.globalOrLocal == 1){
            pAlign.alignLength = rInfo.readSize;
            pAlign.bestNas = (pAlign.alignLength * param.readIdentity) / 100;
            pAlign.bestKmers = pAlign.alignLength - (pAlign.alignLength - pAlign.bestNas) * 4 - 3;
            if (param.readIdentity >= 94) {   // pigeon hole value
                pAlign.bestPigeon = pAlign.alignLength / param.kmerSize - 1 - (pAlign.alignLength - pAlign.bestNas);
                if (pAlign.bestPigeon <= 1 ) pAlign.bestPigeon = 2;
            }
        }

        if (readBinaryBlock(rInfo)!=0){
            return alignmentResult;
        }

        getKmers(rInfo.readSize);

        alignmentResult = alignment(rInfo);

        return alignmentResult;
    }

    private int readBinaryBlock(ReadInfo rInfo){
        /* the same binary operation with RefSeq binary (15nt), but written in a different way */
        int i,j;
        int bInteger =0;	// HashCode binary
        int indexOfBlock =0;	// index number of 15Nt blocks
        int valueNX =0;

        /* positive strain */
        for (i=0; i < rInfo.readSize; i++ ){
            char currentNt = rInfo.read.charAt(i);
            if (currentNt >=256) currentNt = 255;

            bInteger<<=2;
            bInteger|=param.alphaCode[currentNt];

            pAlign.singleNtBit[i] = param.alphaCode[currentNt];
            valueNX += param.alphaCodeNNNNN[currentNt]; // N and X and others... have 1 value while Nts has 0 value

            if ((i+1)%15 == 0){	// Initial another 15 Nt block
                pAlign.bRead[indexOfBlock] = bInteger;
                indexOfBlock++;
                bInteger=0;
            }
        }

        if (valueNX >= pAlign.bestNas){return 1;}	// to many Ns and Xs in the read

        /* Add A\N\X value to fulfil last block */
        /* Doesn`t matter what`s been add, */
        /* because later binary operaion will shift them out */
        if (rInfo.readSize%15!=0){
            bInteger<<=30-(rInfo.readSize%15)*2;
            pAlign.bRead[indexOfBlock] = bInteger;
        }

        /* negative strain, reverse complementary */
        bInteger=0;
        indexOfBlock=0;
        for(j=rInfo.readSize-1,i=1; j>=0; j--,i++){
            char currentNt = rInfo.read.charAt(j);

            bInteger<<=2;
            bInteger|=param.alphaCodeComplement[currentNt];

            pAlign.singleNtBitComplement[i-1]=param.alphaCodeComplement[currentNt];
            if (i%15==0){
                pAlign.bRevRead[indexOfBlock] = bInteger;
                indexOfBlock++;
                bInteger=0;
            }
        }

        if (rInfo.readSize%15!=0){
            bInteger<<=30-(rInfo.readSize%15)*2;
            pAlign.bRevRead[indexOfBlock] = bInteger;
        }

        return 0;
    }

    private void getKmers(int length){
        for (int i=0; i<length - param.kmerSize + 1; i++){
            int j = (i%15 + param.kmerSize)*2; // kmer relative end position on a 15Nt block
            pAlign.kmers[i]= j<=30	// the same operation with load Reference kmer binaries
                    ? pAlign.bRead[i/15]>>(30-j)&param.kmerBits
                    : (pAlign.bRead[i/15]<<(j-30)|pAlign.bRead[i/15+1]>>60-j)&param.kmerBits;

            pAlign.revKmers[i]= j<=30
                    ? pAlign.bRevRead[i/15]>>(30-j)&param.kmerBits
                    : (pAlign.bRevRead[i/15]<<(j-30)|pAlign.bRevRead[i/15+1]>>60-j)&param.kmerBits;
        }
    }

    private List<String> alignment(ReadInfo rInfo){
        int eHSPLength = Arithmetic.expectedHSPLength(param.minor, rInfo.readSize, totalLength, param.pairAlign);
        int eReadLength = Arithmetic.effectiveReadLength(rInfo.readSize, eHSPLength, param.minor);
        long eRefLength = Arithmetic.effectiveRefLength(totalLength, eHSPLength, totalNum, param.minor);

        double eValue;
        int readIdentity;
        int i,j,k,l,mRefBlockLen;
        int[] m;
        int trys, reportRepeatHits;
        int readCoverage;
        double readIdentityDouble;
        double highestIdentity = 0;
        CandidateBlock mRefBlock;
        Qgram qGram;
        List<Qgram> qGramSort = new ArrayList<Qgram>();
        String outputLine = "1";
        List<String> alignResult = new ArrayList<String>();
        reportRepeatHits=1;


        int kmerSkip =1; // how to extend kmers, 1bp per extension
        if (rInfo.readSize >= param.skipThreshold) {kmerSkip =2;} // for longer than 1000, 2bps

        if ((param.chains == 0) || (param.chains == 1)){
            int pigeonfilter = getReadKmerHits(rInfo.readSize, kmerSkip);

            if (param.readIdentity >= 94){
                if (pigeonfilter >= pAlign.bestPigeon){
                    Collections.sort(pAlign.kmerHits);
                    getRefCandidateBlockWithPigeon(rInfo.readSize);
                }
            }else {
                if (pAlign.kmerHits.size() > 1) {
                    Collections.sort(pAlign.kmerHits);
                    getRefCandidateBlock(rInfo.readSize);
                }
            }

            pAlign.kmerHits.clear();
            /* merge adjacent blocks */
            mergeCandidateBlocks();
            pAlign.cRefBlockList.clear();
            /* initial 4 Gram hash table */
            buildFourGram(rInfo.readSize);

            /* loop each blocks and return a qualified qGram */
            for (i=0; i<pAlign.mergeRefBlockList.size();i++){
                mRefBlock = pAlign.mergeRefBlockList.get(i);
                mRefBlockLen = mRefBlock.end - mRefBlock.begin;
                m = BBList.get(mRefBlock.chr).s;

                j =0;
                for (k=mRefBlock.begin; k<mRefBlock.end;k++){
                    l = (k%15 + 1)*2; // block bit relative position
                    pAlign.mRefBlockNtBit[j] = l <=30
                            ? (m[k/15] >> (30-l))&3
                            : (m[k/15]<<(l-30)|m[k/15+1]>>(60-l))&3;
                    j++;
                }

                /* applying q Gram filter and revise band width */
                qGram = new Qgram();
                qGramFilter(rInfo.readSize, mRefBlockLen, qGram);
                if (qGram.qGrams < pAlign.bestKmers) continue;
                qGram.chr = mRefBlock.chr;
                qGram.begin = mRefBlock.begin;
                qGram.end = mRefBlock.end;
                qGramSort.add(qGram);
            }
            pAlign.mergeRefBlockList.clear();

            Collections.sort(qGramSort, new QgramComparator()); // sort all objects by their propertiy "qGrams"
            trys =0;

            for (i=0; i<qGramSort.size();i++){
                if (reportRepeatHits > param.reportRepeatHits && param.reportRepeatHits != 0 && param.reportRepeatHits != 1) break;
                qGram = qGramSort.get(i);
                mRefBlockLen = qGram.end - qGram.begin;
                m = BBList.get(qGram.chr).s;
                j = 0;
                for(k=qGram.begin; k<qGram.end; k++){
                    l = (k%15 + 1)*2;
                    pAlign.mRefBlockNtBit[j] = l <=30
                            ? (m[k/15] >> (30-l))&3
                            : (m[k/15] << (l-30)|m[k/15+1]>>(60-l))&3;
                    j++;
                }

                /* banded alignment */
                int aligned = bandAlignment(qGram, mRefBlockLen, rInfo.readSize);
                if (aligned == 0 ){continue;}
// &from1(talign_info[1]);
// &from2(talign_info[3]);

// &end1(talign_info[2]);
// &end2(talign_info[4]);
                readCoverage = Math.abs(pAlign.endFirst - pAlign.fromFirst) + 1;

                if (param.globalOrLocal == 1){
                    readIdentity = (pAlign.identity*100)/rInfo.readSize;
                    readIdentityDouble = (double)(pAlign.identity * 100)/rInfo.readSize;
                }else{
                    if (readCoverage < pAlign.alignLength){
                        trys++;
                        if (trys > param.maxTrys) break;
                        continue;
                    }
                    readIdentity = (pAlign.identity*100)/pAlign.align;
                    readIdentityDouble = (double)(pAlign.identity*100)/pAlign.align;
                }

                if (readIdentity < param.readIdentity){
                    trys++;
                    if (trys>param.maxTrys) break;
                    continue;
                }

                eValue = Arithmetic.getEValue(pAlign.bestScore, param.minor, param.lambda, eReadLength, eRefLength);
                if (eValue > param.eValue){continue;}
                trys = 0;
                reportRepeatHits++;
                String formatEValue = String.format("%.2e",eValue);
                String formatIdentity = String.format("%.2f",readIdentityDouble);

                if (param.reportRepeatHits==1){
                    if (Double.compare(readIdentityDouble, highestIdentity) > 0){
                        highestIdentity = readIdentityDouble;
                        outputLine = rInfo.readName + "\t" + rInfo.readSize + "nt\t" + formatEValue + "\t"
                                + readCoverage + "\t" + (pAlign.fromFirst + 1) + "\t" + (pAlign.endFirst + 1)
                                + "\t+\t" + formatIdentity + "\t" + listTitle.get(qGram.chr).name
                                + "\t" + (qGram.begin + pAlign.fromSecond + 1) + "\t" + (qGram.begin + pAlign.endSecond + 1);
                    }
                }else {

                    outputLine = rInfo.readName + "\t" + rInfo.readSize + "nt\t" + formatEValue + "\t" + readCoverage + "\t" + (pAlign.fromFirst + 1) + "\t" + (pAlign.endFirst + 1) + "\t+\t" + formatIdentity + "\t" + listTitle.get(qGram.chr).name + "\t" + (qGram.begin + pAlign.fromSecond + 1) + "\t" + (qGram.begin + pAlign.endSecond + 1);

                    alignResult.add(outputLine);
                }

            } // end of foreach qGram
            qGramSort.clear();
        } // end of positive strand

        if ((param.chains == 0) || (param.chains == 2)){
            int pigeonRevfilter = getRevReadKmerHits(rInfo.readSize, kmerSkip);

            if (param.readIdentity >= 94){
                if (pigeonRevfilter >= pAlign.bestPigeon){
                    Collections.sort(pAlign.kmerHits);
                    getRefCandidateBlockWithPigeon(rInfo.readSize);
                }else{
                    pAlign.kmerHits.clear();
                    return alignResult;
                }
            }else {
                if (pAlign.kmerHits.size() > 1) {
                    Collections.sort(pAlign.kmerHits);
                    getRefCandidateBlock(rInfo.readSize);
                }else{
                    pAlign.kmerHits.clear();
                    return alignResult;
                }
            }

            pAlign.kmerHits.clear();
            /* merge adjacent blocks */
            mergeCandidateBlocks();
            pAlign.cRefBlockList.clear();
            /* initial 4 Gram hash table */
            buildRevFourGram(rInfo.readSize);
            /* loop each blocks and return a qualified qGram */
            for (i=0; i<pAlign.mergeRefBlockList.size();i++){
                mRefBlock = pAlign.mergeRefBlockList.get(i);
                mRefBlockLen = mRefBlock.end - mRefBlock.begin;
                m = BBList.get(mRefBlock.chr).s;
                j =0;
                for (k=mRefBlock.begin; k<mRefBlock.end;k++){
                    l = (k%15 + 1)*2; // block bit relative position
                    pAlign.mRefBlockNtBit[j] = l <=30
                            ? (m[k/15] >> (30-l))&3
                            : (m[k/15]<<(l-30)|m[k/15+1]>>(60-l))&3;
                    j++;
                }
                                /* applying q Gram filter and revise band width */
                qGram = new Qgram();
                qGramFilter(rInfo.readSize, mRefBlockLen, qGram);
                if (qGram.qGrams < pAlign.bestKmers) continue;
                qGram.chr = mRefBlock.chr;
                qGram.begin = mRefBlock.begin;
                qGram.end = mRefBlock.end;
                qGramSort.add(qGram);
            }

            pAlign.mergeRefBlockList.clear();
            Collections.sort(qGramSort, new QgramComparator()); // sort all objects by their propertiy "qGrams"
            trys =0;

            for (i=0; i<qGramSort.size();i++){
                if (reportRepeatHits > param.reportRepeatHits && param.reportRepeatHits != 0 && param.reportRepeatHits != 1) break;
                qGram = qGramSort.get(i);
                mRefBlockLen = qGram.end - qGram.begin;
                m = BBList.get(qGram.chr).s;
                j = 0;
                for(k=qGram.begin; k<qGram.end; k++){
                    l = (k%15 + 1)*2;
                    pAlign.mRefBlockNtBit[j] = l <=30
                            ? (m[k/15] >> (30-l))&3
                            : (m[k/15] << (l-30)|m[k/15+1]>>(60-l))&3;
                    j++;
                }

                /* banded alignment */
                int aligned = bandRevAlignment(qGram, mRefBlockLen, rInfo.readSize);
                if (aligned == 0 ){continue;}
                readCoverage = Math.abs(pAlign.endFirst - pAlign.fromFirst) + 1;
                if (param.globalOrLocal == 1){
                    readIdentity = (pAlign.identity*100)/rInfo.readSize;
                    readIdentityDouble = (double)(pAlign.identity * 100)/rInfo.readSize;
                }else{
                    if (readCoverage < pAlign.alignLength){
                        trys++;
                        if (trys > param.maxTrys) break;
                        continue;
                    }
                    readIdentity = (pAlign.identity*100)/pAlign.align;
                    readIdentityDouble = (double)(pAlign.identity*100)/pAlign.align;
                }

                if (readIdentity < param.readIdentity){
                    trys++;
                    if (trys>param.maxTrys) break;
                    continue;
                }

                eValue = Arithmetic.getEValue(pAlign.bestScore, param.minor, param.lambda, eReadLength, eRefLength);
                if (eValue > param.eValue){continue;}
                trys = 0;
                reportRepeatHits++;
                String formatEValue = String.format("%.2e",eValue);
                String formatIdentity = String.format("%.2f",readIdentityDouble);

                if (param.reportRepeatHits==1){
                    if (Double.compare(readIdentityDouble, highestIdentity) > 0){
                        highestIdentity = readIdentityDouble;
                        outputLine = rInfo.readName + "\t" + rInfo.readSize + "nt\t" + formatEValue + "\t"
                                + readCoverage + "\t" + (pAlign.fromFirst + 1) + "\t" + (pAlign.endFirst + 1)
                                + "\t-\t" + formatIdentity + "\t" + listTitle.get(qGram.chr).name
                                + "\t" + (qGram.begin + pAlign.fromSecond + 1) + "\t" + (qGram.begin + pAlign.endSecond + 1);
                    }
                }else {

                    outputLine = rInfo.readName + "\t" + rInfo.readSize + "nt\t" + formatEValue + "\t" + readCoverage + "\t" + (pAlign.fromFirst + 1) + "\t" + (pAlign.endFirst + 1) + "\t-\t" + formatIdentity + "\t" + listTitle.get(qGram.chr).name + "\t" + (qGram.begin + pAlign.fromSecond + 1) + "\t" + (qGram.begin + pAlign.endSecond + 1);
                    alignResult.add(outputLine);
                }
            } // end of qGram loop
        } // end of negative strand

        if (param.reportRepeatHits == 1){
            if (!outputLine.equals("1")) {
                alignResult.add(outputLine);
            }else{
                alignResult.add(rInfo.readName + "\t" + rInfo.readSize + "nt\t" + 1 + "\t" + 0 + "\t" + 0 + "\t" + 0 + "\t-\t" + 0 + "\t" + "*" + "\t" + 0 + "\t" + 0);
            }
        }
        return alignResult;
    }

    private int getReadKmerHits(int length, int kmerSkip){
        int kmerInteger;
        int kmerNumber=0;
        for (int i=0; i<length-param.kmerSize+1; i+=kmerSkip){
            kmerInteger = pAlign.kmers[i];

            if ( (index[kmerInteger].n)==0 )
            {continue;}

            kmerNumber++;
            for(int j=0; j<index[kmerInteger].n; j++){
                int currentLoc = index[kmerInteger].loc[j] - i; // kmer match here, -i is the starting position of read
                currentLoc = currentLoc>=0 ? currentLoc : 0;
                long IdAndLoc = (long)(index[kmerInteger].id[j])<<32|currentLoc; //long64= int32(contigID) + int32(location)
                pAlign.kmerHits.add(IdAndLoc);
            }
        }
        return kmerNumber;
    }

    private int getRevReadKmerHits(int length, int kmerSkip){
        int kmerInteger;
        int kmerRevNumber=0;
        for (int i=0; i<length-param.kmerSize+1; i+=kmerSkip){
            kmerInteger = pAlign.revKmers[i];

            if ( (index[kmerInteger].n)==0 )
            {continue;}

            kmerRevNumber++;
            for(int j=0; j<index[kmerInteger].n; j++){
                int currentLoc = index[kmerInteger].loc[j] - i;
                currentLoc = currentLoc>=0 ? currentLoc : 0;
                long IdAndLoc = (long)(index[kmerInteger].id[j])<<32|currentLoc;
                pAlign.kmerHits.add(IdAndLoc);
            }
        }
        return kmerRevNumber;
    }

    private void getRefCandidateBlockWithPigeon(int length){
        long fHit = pAlign.kmerHits.get(0); // resign value of firstHit(pAlign.kmerHits[0]) to another value to avoid change of pAlign
        int markNewBlock = 0;
        int loc, start;
        CandidateBlock cRefBlock;

        int pigeonHits = 1;

        for (int i =1; i<pAlign.kmerHits.size(); i++){
            if( (pAlign.kmerHits.get(i)-fHit) <= param.kmerSize){
                pigeonHits++;
                if(markNewBlock == 0 && pigeonHits >= pAlign.bestPigeon){
                    cRefBlock = new CandidateBlock();
                    cRefBlock.chr = (int)(fHit>>32);  //long64= int32(contigID) + int32(location)
                    long tempLoc = fHit<<32;//long64= int32(location) + 32(0s)
                    loc = (int)(tempLoc>>32);	//long64=32(0s) + int32(location), then to int
                    start = loc - param.kmerSize;	// one kmer length before
                    cRefBlock.begin = start>0 ? start : 0;
                    cRefBlock.end = loc + length + 2*param.kmerSize;
                    if (cRefBlock.end > (listTitle.get(cRefBlock.chr).size-1) )
                    { cRefBlock.end = listTitle.get(cRefBlock.chr).size; }

                    pAlign.cRefBlockList.add(cRefBlock);
                    markNewBlock =1;
                }
            }else{
                fHit = pAlign.kmerHits.get(i);
                markNewBlock = 0;
                pigeonHits = 1;
            }
        }
    }

    private void getRefCandidateBlock(int length){
        long fHit = pAlign.kmerHits.get(0); // resign value of firstHit(pAlign.kmerHits[0]) to another value to avoid change of pAlign
        int markNewBlock = 0;
        int loc, start;
        CandidateBlock cRefBlock;

        for (int i =1; i<pAlign.kmerHits.size(); i++){
            if( (pAlign.kmerHits.get(i)-fHit) <= param.kmerSize){
                if(markNewBlock == 0){
                    cRefBlock = new CandidateBlock();
                    cRefBlock.chr = (int)(fHit>>32);  //long64= int32(contigID) + int32(location)
                    long tempLoc = fHit<<32;//long64= int32(location) + 32(0s)
                    loc = (int)(tempLoc>>32);	//long64=32(0s) + int32(location), then to int
                    start = loc - param.kmerSize;	// one kmer length before
                    cRefBlock.begin = start>0 ? start : 0;
                    cRefBlock.end = loc + length + 2*param.kmerSize;
                    if (cRefBlock.end > (listTitle.get(cRefBlock.chr).size-1) )
                    { cRefBlock.end = listTitle.get(cRefBlock.chr).size; }

                    pAlign.cRefBlockList.add(cRefBlock);
                    markNewBlock =1;
                }
            }else{
                fHit = pAlign.kmerHits.get(i);
                markNewBlock = 0;
            }
        }
    }

    private void mergeCandidateBlocks(){
        if (pAlign.cRefBlockList.size() > 1){
            CandidateBlock currentCRefBlock;
            currentCRefBlock = pAlign.cRefBlockList.get(0);

            for (int i =1; i<pAlign.cRefBlockList.size(); i++){
                if ( (pAlign.cRefBlockList.get(i).chr!=currentCRefBlock.chr)
                        || currentCRefBlock.end-pAlign.cRefBlockList.get(i).begin < 0){
                    pAlign.mergeRefBlockList.add(currentCRefBlock);
                    currentCRefBlock = pAlign.cRefBlockList.get(i);
                }else{
                    if (currentCRefBlock.end - currentCRefBlock.begin < pAlign.maxReadLength)
                    {currentCRefBlock.end = pAlign.cRefBlockList.get(i).end; }
                }
            }
            pAlign.mergeRefBlockList.add(currentCRefBlock);

        }else if(pAlign.cRefBlockList.size() == 1){
            pAlign.mergeRefBlockList.add(pAlign.cRefBlockList.get(0));
        }
    }

    private void buildRevFourGram(int length){
        int i, j;
        int b;
        int fourMerHashCode;
        int fourMerNum = length - 3;

        /* intiating */
        for (i=0; i<pAlign.fourNtBits; i++){
            pAlign.fourMer[i]= 0;
        }

        /* fourMer frequency */
        for (j=0; j<fourMerNum; j++) {
            fourMerHashCode = pAlign.singleNtBitComplement[j] * pAlign.threeNtBits
                    + pAlign.singleNtBitComplement[j + 1] * pAlign.twoNtBits
                    + pAlign.singleNtBitComplement[j + 2] * pAlign.oneNtBits
                    + pAlign.singleNtBitComplement[j + 3];

            pAlign.fourMer[fourMerHashCode]++;
        }

        /* index of fourMerLoci */
        for (i=0,b=0; i<pAlign.fourNtBits; i++){
            pAlign.fourMerBegin[i]=b;
            b += pAlign.fourMer[i]; // give enough binary space for next kmer
            pAlign.fourMer[i]=0;
        }

        /* location info of fourMers */
        for(j=0; j<fourMerNum; j++){
            fourMerHashCode = pAlign.singleNtBitComplement[j]*pAlign.threeNtBits
                    + pAlign.singleNtBitComplement[j+1]*pAlign.twoNtBits
                    + pAlign.singleNtBitComplement[j+2]*pAlign.oneNtBits
                    + pAlign.singleNtBitComplement[j+3];

            // give enough binary space for next kmer
            pAlign.fourMerLoci[pAlign.fourMerBegin[fourMerHashCode]+pAlign.fourMer[fourMerHashCode]] = j;
            pAlign.fourMer[fourMerHashCode]++;
        }
    }

    private void buildFourGram(int length){
        int i, j;
        int b;
        int fourMerHashCode;
        int fourMerNum = length - 3;

        /* intiating */
        for (i=0; i<pAlign.fourNtBits; i++){
            pAlign.fourMer[i]= 0;
        }

        /* fourMer frequency */
        for (j=0; j<fourMerNum; j++){
            fourMerHashCode = pAlign.singleNtBit[j]*pAlign.threeNtBits
                    + pAlign.singleNtBit[j+1]*pAlign.twoNtBits
                    + pAlign.singleNtBit[j+2]*pAlign.oneNtBits
                    + pAlign.singleNtBit[j+3];

            pAlign.fourMer[fourMerHashCode]++;
        }

        /* index of fourMerLoci */
        for (i=0,b=0; i<pAlign.fourNtBits; i++){
            pAlign.fourMerBegin[i]=b;
            b += pAlign.fourMer[i]; // give enough binary space for next kmer
            pAlign.fourMer[i]=0;
        }

        /* location info of fourMers */
        for(j=0; j<fourMerNum; j++){
            fourMerHashCode = pAlign.singleNtBit[j]*pAlign.threeNtBits
                    + pAlign.singleNtBit[j+1]*pAlign.twoNtBits
                    + pAlign.singleNtBit[j+2]*pAlign.oneNtBits
                    + pAlign.singleNtBit[j+3];

            // give enough binary space for next kmer
            pAlign.fourMerLoci[pAlign.fourMerBegin[fourMerHashCode]+pAlign.fourMer[fourMerHashCode]] = j;
            pAlign.fourMer[fourMerHashCode]++;
        }
    }

    private void qGramFilter(int readLength, int mRefBlockLen, Qgram qGram){
        int i, j, k, l;
        int cRefFourMerNum = mRefBlockLen - 3;
        int readNtPosition = readLength - 1;
        int currentFourMerHashCode;
        int totalLengthTogether = readLength + mRefBlockLen - 1;
        int fourMerLociIndex;

        for (i=totalLengthTogether,l=0; i>0; i--,l++){
            pAlign.fourMerScore[l]=0;	// initial kmer score matrix
        }

        /* band score */
        for (i=0,l=0; i<cRefFourMerNum; i++, readNtPosition++, l++){
            currentFourMerHashCode = pAlign.mRefBlockNtBit[l+0]*pAlign.threeNtBits
                    + pAlign.mRefBlockNtBit[l+1]*pAlign.twoNtBits
                    + pAlign.mRefBlockNtBit[l+2]*pAlign.oneNtBits
                    + pAlign.mRefBlockNtBit[l+3];
            if (pAlign.fourMer[currentFourMerHashCode] == 0)
            {continue;}
            fourMerLociIndex = pAlign.fourMerBegin[currentFourMerHashCode];
            for (j=pAlign.fourMer[currentFourMerHashCode]; j>0; j--, fourMerLociIndex++){
                // readNtPosition-pAlign.fourMerLoci[fourMerLociIndex] represents the distance of the band
                pAlign.fourMerScore[readNtPosition-pAlign.fourMerLoci[fourMerLociIndex]]++;
            }
        }

        int requiredNts = pAlign.bestNas;
        int bandB = requiredNts >= 1 ? requiredNts - 1: 0;
        int bandE = totalLengthTogether - requiredNts;
        int bandM = (bandB + param.bandWidth-1<bandE) ? bandB+param.bandWidth-1 : bandE;
        int bestScore = 0;
        for(i=bandB; i<bandM+1;i++){
            bestScore += pAlign.fourMerScore[i];
        }


        /* band width revise */
        int from = bandB;
        int end = bandM;
        int score = bestScore;
        for(k=from ,j=bandM + 1; j<bandE; j++){
            score -= pAlign.fourMerScore[k++];
            score += pAlign.fourMerScore[j];
            if (score > bestScore){
                from =k;
                end = j;
                bestScore = score;
            }
        }

        for (j=from; j<=end; j++){
            if (pAlign.fourMerScore[j] < 5){
                bestScore -= pAlign.fourMerScore[j];
                from++;
            }else break;
        }

        for (j=end; j>=from; j--){
            if (pAlign.fourMerScore[j] < 5){
                bestScore -= pAlign.fourMerScore[j];
                end--;
            }else break;
        }

        qGram.bandLeft = from - readLength + 1;
        qGram.bandRight = end - readLength + 1;

        qGram.qGrams = bestScore;
    }

    private int bandRevAlignment(Qgram qGram, int mRefBlockLen, int readLength){
        int i, j, k, l;
        int js, ks;
        int bestScoreTemp;
        int identityTemp;
        int bestFromFirst, bestFromSecond, bestAlign;

        pAlign.identity =0;
        pAlign.fromFirst =0;
        pAlign.fromSecond =0;

        if ( (qGram.bandRight >= mRefBlockLen) || (qGram.bandLeft <= -readLength) || (qGram.bandLeft > qGram.bandRight))
        {return 0;}

        int bandWidth = qGram.bandRight - qGram.bandLeft +1;
        for (i=0;i<readLength;i++){
            for (l=0;l<bandWidth; l++){
                pAlign.scoreMatrix[i][l] = 0;
            }
        }

        pAlign.bestScore = 0;
        /* initial left side score of matrix */
        if (qGram.bandLeft < 0){
            int tempBand = (qGram.bandRight < 0) ? qGram.bandRight : 0;
            for (k=qGram.bandLeft; k<=tempBand; k++){
                i = -k;
                l = k - qGram.bandLeft;
                if ((pAlign.scoreMatrix[i][l] = mat.matrix[pAlign.singleNtBitComplement[i]][pAlign.mRefBlockNtBit[0]]) > pAlign.bestScore){
                    pAlign.bestScore = pAlign.scoreMatrix[i][l];
                    pAlign.fromFirst = i ; pAlign.fromSecond=0; pAlign.endFirst = i; pAlign.endSecond=0; pAlign.align = 1;
                }
                pAlign.identityMatrix[i][l] = (pAlign.singleNtBitComplement[i] == pAlign.mRefBlockNtBit[0]) ? 1 : 0;
                pAlign.fromFirstMatrix[i][l] = i;
                pAlign.fromSecondMatrix[i][l] = 0;
                pAlign.alignMatrix[i][l] = 1;
            }
        }

        /* initial up side score of matrix */
        if (qGram.bandRight >=0){
            int tempBand = (qGram.bandLeft > 0) ? qGram.bandLeft : 0;
            for (i=0, j=tempBand; j<=qGram.bandRight; j++){
                l = j - qGram.bandLeft;
                if ((pAlign.scoreMatrix[i][l] = mat.matrix[pAlign.singleNtBitComplement[i]][pAlign.mRefBlockNtBit[j]]) > pAlign.bestScore){
                    pAlign.bestScore = pAlign.scoreMatrix[i][l];
                    pAlign.fromFirst = i ; pAlign.fromSecond=j; pAlign.endFirst = i; pAlign.endSecond =j; pAlign.align = 0;
                }
                pAlign.identityMatrix[i][l] = (pAlign.singleNtBitComplement[i] == pAlign.mRefBlockNtBit[j]) ? 1 : 0;
                pAlign.fromFirstMatrix[i][l] = i;
                pAlign.fromSecondMatrix[i][l] = j;
                pAlign.alignMatrix[i][l] =1;
            }
        }

        for (i=1; i<readLength; i++){
            for(l=0; l<bandWidth; l++){
                j = l + i + qGram.bandLeft;
                if (j<1 || j>=mRefBlockLen) continue;

                if ( (bestScoreTemp = pAlign.scoreMatrix[i-1][l]) > 0){
                    identityTemp = pAlign.identityMatrix[i-1][l];
                    bestFromFirst = pAlign.fromFirstMatrix[i-1][l];
                    bestFromSecond = pAlign.fromSecondMatrix[i-1][l];
                    bestAlign = pAlign.alignMatrix[i-1][l] + 1;
                }else{
                    bestScoreTemp = 0;
                    identityTemp = 0;
                    bestFromFirst = i;
                    bestFromSecond = j;
                    bestAlign = 1;
                }


                int n, p;
                p = (-qGram.bandLeft+1-i > 0) ? -qGram.bandLeft+1-i : 0 ;
                for (k = l-1, ks=0; k>=p; k--, ks++){
                    if ((n = pAlign.scoreMatrix[i-1][k] + mat.gapArray[ks]) > bestScoreTemp){
                        bestScoreTemp = n;
                        identityTemp = pAlign.identityMatrix[i-1][k];
                        bestFromFirst = pAlign.fromFirstMatrix[i-1][k];
                        bestFromSecond = pAlign.fromSecondMatrix[i-1][k];
                        bestAlign = pAlign.alignMatrix[i-1][k] + ks + 2;
                    }
                }

                p = (j-qGram.bandRight-1 > 0) ? j-qGram.bandRight -1 : 0;
                for (k=i-2, js=l+1, ks=0; k>=p; k--,ks++,js++){
                    if ((n = pAlign.scoreMatrix[k][js] + mat.gapArray[ks]) > bestScoreTemp){
                        bestScoreTemp = n;
                        identityTemp = pAlign.identityMatrix[k][js];
                        bestFromFirst = pAlign.fromFirstMatrix[k][js];
                        bestFromSecond = pAlign.fromSecondMatrix[k][js];
                        bestAlign = pAlign.alignMatrix[k][js] + ks + 2;
                    }
                }

                bestScoreTemp += mat.matrix[pAlign.singleNtBitComplement[i]][pAlign.mRefBlockNtBit[j]];

                if (pAlign.singleNtBitComplement[i] == pAlign.mRefBlockNtBit[j]){identityTemp++;}

                pAlign.scoreMatrix[i][l] = bestScoreTemp;
                pAlign.identityMatrix[i][l] = identityTemp;
                pAlign.fromFirstMatrix[i][l] = bestFromFirst;
                pAlign.fromSecondMatrix[i][l] = bestFromSecond;
                pAlign.alignMatrix[i][l] = bestAlign;

                if (bestScoreTemp > pAlign.bestScore){
                    pAlign.bestScore = bestScoreTemp;
                    pAlign.identity = identityTemp;
                    pAlign.endFirst = i;
                    pAlign.endSecond = j;
                    pAlign.fromFirst = bestFromFirst;
                    pAlign.fromSecond = bestFromSecond;
                    pAlign.align = bestAlign;
                }
            }
        }
        return 1;
    }

    private int bandAlignment(Qgram qGram, int mRefBlockLen, int readLength){
        int i, j, k, l;
        int js, ks;
        int bestScoreTemp;
        int identityTemp;
        int bestFromFirst, bestFromSecond, bestAlign;

        pAlign.identity =0;
        pAlign.fromFirst =0;
        pAlign.fromSecond =0;

        if ( (qGram.bandRight >= mRefBlockLen) || (qGram.bandLeft <= -readLength) || (qGram.bandLeft > qGram.bandRight))
        {return 0;}

        int bandWidth = qGram.bandRight - qGram.bandLeft +1;
        for (i=0;i<readLength;i++){
            for (l=0;l<bandWidth; l++){
                pAlign.scoreMatrix[i][l] = 0;
            }
        }

        pAlign.bestScore = 0;
        /* initial left side score of matrix */
        if (qGram.bandLeft < 0){
            int tempBand = (qGram.bandRight < 0) ? qGram.bandRight : 0;
            for (k=qGram.bandLeft; k<=tempBand; k++){
                i = -k;
                l = k - qGram.bandLeft;
                if ((pAlign.scoreMatrix[i][l] = mat.matrix[pAlign.singleNtBit[i]][pAlign.mRefBlockNtBit[0]]) > pAlign.bestScore){
                    pAlign.bestScore = pAlign.scoreMatrix[i][l];
                    pAlign.fromFirst = i ; pAlign.fromSecond=0; pAlign.endFirst = i; pAlign.endSecond=0; pAlign.align = 1;
                }
                pAlign.identityMatrix[i][l] = (pAlign.singleNtBit[i] == pAlign.mRefBlockNtBit[0]) ? 1 : 0;
                pAlign.fromFirstMatrix[i][l] = i;
                pAlign.fromSecondMatrix[i][l] = 0;
                pAlign.alignMatrix[i][l] = 1;
            }
        }

        /* initial up side score of matrix */
        if (qGram.bandRight >=0){
            int tempBand = (qGram.bandLeft > 0) ? qGram.bandLeft : 0;
            for (i=0, j=tempBand; j<=qGram.bandRight; j++){
                l = j - qGram.bandLeft;
                if ((pAlign.scoreMatrix[i][l] = mat.matrix[pAlign.singleNtBit[i]][pAlign.mRefBlockNtBit[j]]) > pAlign.bestScore){
                    pAlign.bestScore = pAlign.scoreMatrix[i][l];
                    pAlign.fromFirst = i ; pAlign.fromSecond=j; pAlign.endFirst = i; pAlign.endSecond =j; pAlign.align = 0;
                }
                pAlign.identityMatrix[i][l] = (pAlign.singleNtBit[i] == pAlign.mRefBlockNtBit[j]) ? 1 : 0;
                pAlign.fromFirstMatrix[i][l] = i;
                pAlign.fromSecondMatrix[i][l] = j;
                pAlign.alignMatrix[i][l] =1;
            }
        }

        for (i=1; i<readLength; i++){
            for(l=0; l<bandWidth; l++){
                j = l + i + qGram.bandLeft;
                if (j<1 || j>=mRefBlockLen) continue;

                if ( (bestScoreTemp = pAlign.scoreMatrix[i-1][l]) > 0){
                    identityTemp = pAlign.identityMatrix[i-1][l];
                    bestFromFirst = pAlign.fromFirstMatrix[i-1][l];
                    bestFromSecond = pAlign.fromSecondMatrix[i-1][l];
                    bestAlign = pAlign.alignMatrix[i-1][l] + 1;
                }else{
                    bestScoreTemp = 0;
                    identityTemp = 0;
                    bestFromFirst = i;
                    bestFromSecond = j;
                    bestAlign = 1;
                }

                int n, p;
                p = (-qGram.bandLeft+1-i > 0) ? -qGram.bandLeft+1-i : 0 ;
                for (k = l-1, ks=0; k>=p; k--, ks++){
                    if ((n = pAlign.scoreMatrix[i-1][k] + mat.gapArray[ks]) > bestScoreTemp){
                        bestScoreTemp = n;
                        identityTemp = pAlign.identityMatrix[i-1][k];
                        bestFromFirst = pAlign.fromFirstMatrix[i-1][k];
                        bestFromSecond = pAlign.fromSecondMatrix[i-1][k];
                        bestAlign = pAlign.alignMatrix[i-1][k] + ks + 2;
                    }
                }

                p = (j-qGram.bandRight-1 > 0) ? j-qGram.bandRight -1 : 0;
                for (k=i-2, js=l+1, ks=0; k>=p; k--,ks++,js++){
                    if ((n = pAlign.scoreMatrix[k][js] + mat.gapArray[ks]) > bestScoreTemp){
                        bestScoreTemp = n;
                        identityTemp = pAlign.identityMatrix[k][js];
                        bestFromFirst = pAlign.fromFirstMatrix[k][js];
                        bestFromSecond = pAlign.fromSecondMatrix[k][js];
                        bestAlign = pAlign.alignMatrix[k][js] + ks + 2;
                    }
                }

                bestScoreTemp += mat.matrix[pAlign.singleNtBit[i]][pAlign.mRefBlockNtBit[j]];

                if (pAlign.singleNtBit[i] == pAlign.mRefBlockNtBit[j]){identityTemp++;}

                pAlign.scoreMatrix[i][l] = bestScoreTemp;
                pAlign.identityMatrix[i][l] = identityTemp;
                pAlign.fromFirstMatrix[i][l] = bestFromFirst;
                pAlign.fromSecondMatrix[i][l] = bestFromSecond;
                pAlign.alignMatrix[i][l] = bestAlign;

                if (bestScoreTemp > pAlign.bestScore){
                    pAlign.bestScore = bestScoreTemp;
                    pAlign.identity = identityTemp;
                    pAlign.endFirst = i;
                    pAlign.endSecond = j;
                    pAlign.fromFirst = bestFromFirst;
                    pAlign.fromSecond = bestFromSecond;
                    pAlign.align = bestAlign;
                }
            } // end of loop l
        } // end of loop i
        return 1;
    } // end of bandAlignment

    /**
     * This method sets the input parameters.
     *
     * @param param {@link DefaultParam}.
     */
    public void setParam(DefaultParam param){
        this.param = param;
        setAlignmentParameter();
    }

    /**
     * This method sets the alignment parameters {@link AlignmentParameter}.
     */
    public void setAlignmentParameter(){
        this.pAlign = new AlignmentParameter(param);
    }

    /**
     * This method passes the reference index to the recruitment pipeline.
     *
     * @param ref the reference index.
     */
    public void setStruct(RefStructBuilder ref){
        this.BBList = ref.BBList;
        this.listTitle = ref.title;
        this.index = ref.index;
        this.totalLength = ref.totalLength;
        this.totalNum = ref.totalNum;
    }

    /**
     * This method sets the scoring matrix.
     *
     * @param mat the scoring matrix {@link ScoreMatrix}.
     */
    public void setMatrix(ScoreMatrix mat){
        this.mat = mat;
    }

}
