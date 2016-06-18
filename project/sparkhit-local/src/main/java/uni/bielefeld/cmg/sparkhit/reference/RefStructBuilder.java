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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Liren Huang on 17/02/16.
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


public class RefStructBuilder implements RefStructManager{
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
     *
     * @param param
     */
    public void setParameter(DefaultParam param){
        this.param = param;
        this.sizeKmers = param.kmerSize;
        this.alphaCode = param.alphaCode;
        this.maximumKmerNum = param.maximumKmerNum;
        this.index = new KmerLoc[maximumKmerNum];
    }

    /**
     *
     * @param fasta
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
     *
     * @param s
     * @param n
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
	/* 12 Nts stored as a block of 24 bits */
	/* (but stored as Integer (primitive type 32 bits)) */

    /**
     *
     * @return
     */
    public BinaryBlock getBinarySeq(){

   		/* 12nt form a block of 24bits, but stored in a 32bit Integer primitive tpye */
		/* +11 to built a block, +2 for 3`end overflow when extract binary code */
        BinaryBlock bBlock = new BinaryBlock();
        bBlock.n = (length + 11) / 12 + 2;

        int remainder = bBlock.n * 12 - length; // how many cells left for the last block
        if (remainder > 0 ){
            StringBuilder Nremainder = repeatSb("N", remainder);
            seqBuilder.append(Nremainder);
        }

        bBlock.s = new int[bBlock.n]; // initiating bBlock.n blocks, stored as Integer in bBlock.s array

        int bBlockSize = 0;
        for (int i = 0; i < bBlock.n ; i++, bBlockSize += 12){
            bBlock.s[i] = 0;
            for (int j = 0; j <12 ; j++){
                bBlock.s[i] <<= 2;
                char currentNt = seqBuilder.charAt(bBlockSize + j);
                bBlock.s[i] |= alphaCode[currentNt];
            }
        }

        return bBlock;
    }

    /**
     *
     * @param fasta
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
     *
     * @param validNt
     * @param fromIndex
     * @return
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
     *
     * @param inputFaPath
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
                l = (j%12 + sizeKmers)*2;	// RELATIVE location of the last kmer Nt

                kmerInteger = l<=24		// whether the end of the kmer is in the same block with the start of the kmer
								/* |------------|     binary block                    */
								/*    ---------       kmer                            */
								/*  -----------.      binary shifting                 */
								/*  ..---------.      &, AND operation with maximum of kmerBits */
                        ? (m[j/12] >> (24-l))
                        &param.kmerBits
								/* |------------|------------|     binary block       */
								/*            ---------            kmer		      */
								/*  ------------+------            first, binary left shift     */
								/*               ------......      second, binary right shift   */
								/*  -------------------......      |, OR operation    */
								/*  ..........---------......      &, AND operation with maximum*/
                        : (m[j/12] << (l-24) | m[j/12+1] >> (48-l))
                        &param.kmerBits;

                index[kmerInteger].n++;
            }

            if ( ((b.end-b.begin) - sizeKmers) % skip != 0){		/* |------------|------------|        binary block */
										/* -------------------------|         masked block */
										/* 		  ---------           kmer         */
										/*                   ---------        skip out of block, last Nt unlog */
                j = b.end - sizeKmers;				// <--             ---------          move back to log the last kmer
                //				      So here the last kmer migh skip smaller than sizeKmers - param.kmerOverlap
                l = (j%12 + sizeKmers) * 2;
                kmerInteger = l<=24
                        ? (m[j/12] >> (24-l))
                        &param.kmerBits
                        : (m[j/12] << (l-24) | m[j/12+1] >> (48-l))
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
                l = (j%12 + sizeKmers)*2;

				/* the same as initiating index */
                kmerInteger = l<=24
                        ? (m[j/12] >> (24-l))
                        &param.kmerBits
                        : (m[j/12] << (l-24) | m[j/12+1] >> (48-l))
                        &param.kmerBits;

				/* index loci is an array now. So */
                index[kmerInteger].loc[index[kmerInteger].n] = j;
                index[kmerInteger].id[index[kmerInteger].n] = b.id;
                index[kmerInteger].n++;
            }

            if ( ((b.end-b.begin) - sizeKmers) % skip != 0){
                j = b.end - sizeKmers;

                l = (j%12 + sizeKmers) * 2;
                kmerInteger = l<=24
                        ? (m[j/12] >> (24-l))
                        &param.kmerBits
                        : (m[j/12] << (l-24) | m[j/12+1] >> (48-l))
                        &param.kmerBits;

                index[kmerInteger].loc[index[kmerInteger].n] = j;
                index[kmerInteger].id[index[kmerInteger].n] = b.id;
                index[kmerInteger].n++;
            }
        }
    }

    /**
     *
     */
    public void buildIndex(){
        initialIndex();
        getKmerFreq();
        getKmerLociIndex();
        loadIndex();
    }
}
