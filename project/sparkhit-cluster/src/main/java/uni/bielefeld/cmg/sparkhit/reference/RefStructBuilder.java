package uni.bielefeld.cmg.sparkhit.reference;


import uni.bielefeld.cmg.sparkhit.io.ReadFasta;
import uni.bielefeld.cmg.sparkhit.struct.BinaryBlock;
import uni.bielefeld.cmg.sparkhit.struct.Block;
import uni.bielefeld.cmg.sparkhit.struct.KmerLoc;
import uni.bielefeld.cmg.sparkhit.struct.RefTitle;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rhinempi on 22.07.2017.
 *
 *       Sparkhit
 *
 * Copyright (c) 2017.
 *       Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Returns an object for building reference index.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class RefStructBuilder implements RefStructManager, Serializable{
    /* parameters */
    private DefaultParam param;
    private int sizeKmers;
    private int[] alphaCode;
    private int maximumKmerNum;

    /* info dumper */
    private InfoDumper info = new InfoDumper();

    /* data structures */
    public List<BinaryBlock> BBList = new ArrayList<BinaryBlock>();
    public List<Block> block = new ArrayList<Block>();
    public List<RefTitle> title = new ArrayList<RefTitle>();
    public KmerLoc[] index;
    public long totalLength = 0;
    public int totalNum;

    /* global variables */
    private String name;
    private int length = 0;
    private int contigId = 0;
    private String seq = "";
    private StringBuilder seqBuilder = new StringBuilder();

    /**
     * This method sets all correspond parameters for reference
     * data structure construction.
     *
     * @param param {@link DefaultParam}.
     */
    public void setParameter(DefaultParam param){
        this.param = param;
        this.sizeKmers = param.kmerSize;
        this.alphaCode = param.alphaCode;
        this.maximumKmerNum = param.maximumKmerNum;
        this.index = new KmerLoc[maximumKmerNum];
    }

    /**
     * This method loads the first line of a fasta file.
     *
     * @param fasta {@link BufferedReader}.
     */
    private void loadFirstLine(BufferedReader fasta){
        String line;
        try {
            if ((line = fasta.readLine()) != null){
                if (!line.startsWith(">")){ // first time encounter a ">"
                    info.readMessage("Input reference file is not fasta format");
                    info.screenDump();
                }else{
                    String[] head = line.split("\\s+");
                    name = head[0].substring(1);
                }
            }else{
                info.readMessage("Input reference file empty!");
                info.screenDump();
                System.exit(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method uses {@link StringBuilder} to concatenate repeat sequences.
     *
     * @param s a reference sequence string.
     * @param n the size of the repeat.
     * @return
     */
    public StringBuilder repeatSb(String s, int n){
        StringBuilder sb = new StringBuilder();
        for(int i = 0 ; i < n ; i++){
            sb.append(s);
        }
        return sb;
    }

    /* creat binary sequence */
	/* convert string sequence to binary storage */
	/* one Nucleotide takes 2 bits */
	/* 15 Nts stored as a block of 30 bits */
	/* (but stored as Integer (primitive type 32 bits)) */

    /**
     * This method transforms a nucletotide sequence from a string type to a binary type.
     *
     * @return {@link BinaryBlock}.
     */
    public BinaryBlock getBinarySeq(){

   		/* 15nt form a block of 30bits, but stored in a 32bit Integer primitive tpye */
		/* +14 to built a block, +2 for 3`end overflow when extract binary code */
        BinaryBlock bBlock = new BinaryBlock();
        bBlock.n = (length + 14) / 15 + 2;

        int remainder = bBlock.n * 15 - length; // how many cells left for the last block
        if (remainder > 0 ){
            StringBuilder Nremainder = repeatSb("N", remainder);
            seqBuilder.append(Nremainder);
        }

        bBlock.s = new int[bBlock.n]; // initiating bBlock.n blocks, stored as Integer in bBlock.s array

        int bBlockSize = 0;
        for (int i = 0; i < bBlock.n ; i++, bBlockSize += 15){
            bBlock.s[i] = 0;
            for (int j = 0; j <15 ; j++){
                bBlock.s[i] <<= 2;
                char currentNt = seqBuilder.charAt(bBlockSize + j);
                bBlock.s[i] |= alphaCode[currentNt];
            }
        }

        return bBlock;
    }

    /**
     * This method loads the all contigs from a reference genome.
     *
     * @param fasta {@link BufferedReader}.
     */
    private void loadContig(BufferedReader fasta){
        String line;
        try {
            while((line = fasta.readLine()) != null){
                if (line.startsWith(">")){
                    RefTitle rRefTitle = new RefTitle();
                    rRefTitle.name = name;      /* these are information of former contig */
                    rRefTitle.size = length;    /* these are information of former contig */
                    title.add(rRefTitle);       /* these are information of former contig */

                    BinaryBlock bBlock;
                    bBlock = getBinarySeq();
                    BBList.add(bBlock);

                    seq = seqBuilder.toString();
                    unMask();

                    contigId++;
                    totalNum++;
                    totalLength += length;
                    length = 0;
                    seqBuilder = new StringBuilder();

                    String[] head = line.split("\\s+");
                    name = head[0].substring(1);
                }else{
                    length += line.length();
                    seqBuilder.append(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method returns the index of a specified pattern.
     *
     * @param validNt the pattern {@link Pattern}, which its index is searched against.
     * @param fromIndex the starting point for searching the index.
     * @return the index of the searching result.
     */
    private int myIndexOfAny(Pattern validNt, int fromIndex){
        int position = length + 1;	// outside, longer than contiglength
        Matcher matcher = validNt.matcher(seq);
        if (matcher.find(fromIndex)){
            position = matcher.start();
        }
        return position;
    }

    /**
     *
     */
    private void unMask(){
        Block b = new Block();
        b.id = contigId;
        b.begin = b.end = 0; // initial block position

        while (b.end < length){

            b.begin = myIndexOfAny(param.validNt, b.end);
            if (b.begin > length) break;

            b.end = myIndexOfAny(param.invalidNt, b.begin);
            b.end = (b.end <= length ? b.end : length);

            if (	(!block.isEmpty())				// not the first block
                    && (b.id==block.get(block.size()-1).id)		// the same contig, id is the num mark of contig or binarySeq
                    && (b.begin - block.get(block.size()-1).end < 5)	// repeat N or X smaller than 5, N and X are equal to 'A'
                    ){
                block.get(block.size()-1).end = b.end;
            }else{
                if (b.end - b.begin < 20) {continue;}
                block.add(b);

				/* re-initial Block b */
                b = new Block();
                b.id = contigId;
                b.begin = block.get(block.size()-1).begin;
                b.end = block.get(block.size()-1).end;
            }
        }
    }

    /**
     *
     */
    private void logLastContig(){
        RefTitle rRefTitle = new RefTitle();
        rRefTitle.name = name;	/* these are information of THE last contig*/
        rRefTitle.size = length;/* these are information of THE last contig*/
        title.add(rRefTitle);	/* these are information of THE last contig*/

        BinaryBlock bBlock;
        bBlock = getBinarySeq();
        BBList.add(bBlock);

        seq = seqBuilder.toString();
        unMask();

        contigId++;
        totalNum++;
        totalLength += length;
    }

    /**
     * This method load the genome sequences from the input reference file.
     *
     * @param inputFaPath the full path of the input file for reference genomes.
     */
    public void loadRef (String inputFaPath){
        ReadFasta refReader = new ReadFasta();
        refReader.bufferInputFile(inputFaPath);
        BufferedReader fasta = refReader.getFastaBufferedReader();

        loadFirstLine(fasta);
        loadContig(fasta);

        try {
            fasta.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logLastContig();
    }

    /**
     *
     */
    private void initialIndex(){
        for (int i=0; i<maximumKmerNum; i++){
            index[i] = new KmerLoc();
            index[i].n = 0;
        }
    }

    /**
     *
     */
    private void getKmerFreq(){
        int[] m;
        Block b;
        int j, l, kmerInteger;
        int skip = sizeKmers - param.kmerOverlap;

        for (int i=0; i<block.size(); i++){
            b = block.get(i);
            m = BBList.get(b.id).s;

            for (j = b.begin; j< (b.end-sizeKmers+1); j += skip){
                l = (j%15 + sizeKmers)*2;	// RELATIVE location of the last kmer Nt

                kmerInteger = l<=30		// whether the end of the kmer is in the same block with the start of the kmer
								/* |------------|     binary block                    */
								/*    ---------       kmer                            */
								/*  -----------.      binary shifting                 */
								/*  ..---------.      &, AND operation with maximum of kmerBits */
                        ? (m[j/15] >> (30-l))
                        &param.kmerBits
								/* |------------|------------|     binary block       */
								/*            ---------            kmer		      */
								/*  ------------+------            first, binary left shift     */
								/*               ------......      second, binary right shift   */
								/*  -------------------......      |, OR operation    */
								/*  ..........---------......      &, AND operation with maximum*/
                        : (m[j/15] << (l-30) | m[j/15+1] >> (60-l))
                        &param.kmerBits;

                index[kmerInteger].n++;
            }

            if ( ((b.end-b.begin) - sizeKmers) % skip != 0){		/* |------------|------------|        binary block */
										/* -------------------------|         masked block */
										/* 		  ---------           kmer         */
										/*                   ---------        skip out of block, last Nt unlog */
                j = b.end - sizeKmers;				// <--             ---------          move back to log the last kmer
                //				      So here the last kmer migh skip smaller than sizeKmers - param.kmerOverlap
                l = (j%15 + sizeKmers) * 2;
                kmerInteger = l<=30
                        ? (m[j/15] >> (30-l))
                        &param.kmerBits
                        : (m[j/15] << (l-30) | m[j/15+1] >> (60-l))
                        &param.kmerBits;

                index[kmerInteger].n++;
            }
        }
    }

    /**
     *
     */
    private void getKmerLociIndex(){
        for (int i = 0; i < maximumKmerNum; i++ ){
            if (index[i].n > 0){
                index[i].id = new int[index[i].n];	// how many IDs (contigs) for one kmer
                index[i].loc = new int[index[i].n];	// how many Locations for one kmer
                index[i].n = 0;				// reset to 0, reload when loading index
            }
        }
    }

    /**
     *
     */
    private void loadIndex(){
        int[] m;
        int j, l, kmerInteger;
        Block b;
        int skip = sizeKmers - param.kmerOverlap;
        for (int i = 0; i < block.size(); i++){
            b = block.get(i);
            m = BBList.get(b.id).s;

            for (j=b.begin; j < (b.end - sizeKmers +1); j+=skip){
                l = (j%15 + sizeKmers)*2;

				/* the same as initiating index */
                kmerInteger = l<=30
                        ? (m[j/15] >> (30-l))
                        &param.kmerBits
                        : (m[j/15] << (l-30) | m[j/15+1] >> (60-l))
                        &param.kmerBits;

				/* index loci is an array now. So */
                index[kmerInteger].loc[index[kmerInteger].n] = j;
                index[kmerInteger].id[index[kmerInteger].n] = b.id;
                index[kmerInteger].n++;
            }

            if ( ((b.end-b.begin) - sizeKmers) % skip != 0){
                j = b.end - sizeKmers;

                l = (j%15 + sizeKmers) * 2;
                kmerInteger = l<=30
                        ? (m[j/15] >> (30-l))
                        &param.kmerBits
                        : (m[j/15] << (l-30) | m[j/15+1] >> (60-l))
                        &param.kmerBits;

                index[kmerInteger].loc[index[kmerInteger].n] = j;
                index[kmerInteger].id[index[kmerInteger].n] = b.id;
                index[kmerInteger].n++;
            }
        }
    }

    /**
     * This method build the reference index based on the input
     * reference genomes.
     */
    public void buildIndex(){
        initialIndex();
        getKmerFreq();
        getKmerLociIndex();
        loadIndex();
    }
}
