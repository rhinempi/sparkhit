package uni.bielefeld.cmg.sparkhit.struct;

import uni.bielefeld.cmg.sparkhit.util.DefaultParam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
public class AlignmentParameter implements Serializable{
    final public int maxReadLength = 500;
    final public int maxBlockLength = 2*maxReadLength +12*3;
    final public int oneNtBits = 4;
    final public int twoNtBits = oneNtBits * oneNtBits;
    final public int threeNtBits = twoNtBits * oneNtBits;
    final public int fourNtBits = threeNtBits * threeNtBits;
    final int maxNxM = (1<<(oneNtBits*4)) -1; // O(n*m) complexity

    public int[] kmers =new int[maxReadLength];
    public int[] revKmers =new int[maxReadLength];

    public int[] bRead =new int[maxReadLength/12+1];
    public int[] bRevRead =new int[maxReadLength/12+1];

    public int[] fourMer =new int[fourNtBits];
    public int[] fourMerBegin =new int[fourNtBits];
    public int[] fourMerLoci =new int[maxNxM];
    public int[] fourMerScore = new int[maxNxM];

    public List<Long> kmerHits = new ArrayList<Long>();
    public List<CandidateBlock> cRefBlockList = new ArrayList<CandidateBlock>();
    public List<CandidateBlock> mergeRefBlockList = new ArrayList<CandidateBlock>();

    public int[] singleNtBit = new int[maxReadLength+12];   // read bit seq
    public int[] singleNtBitComplement = new int[maxReadLength+12];

    public int[] mRefBlockNtBit = new int[maxBlockLength];

    public int[][] scoreMatrix = new int[maxBlockLength][maxBlockLength];

    public int alignLength;
    public int bestNas;
    public int bestKmers;

    public int bestPigeon;

    public int fromFirst;
    public int fromSecond;
    public int endFirst;
    public int endSecond;
    public int align;
    public int[][] identityMatrix = new int[maxBlockLength][maxBlockLength];
    public int[][] fromFirstMatrix = new int[maxBlockLength][maxBlockLength];
    public int[][] fromSecondMatrix = new int[maxBlockLength][maxBlockLength];
    public int[][] alignMatrix = new int[maxBlockLength][maxBlockLength];
    public int identity;
    public int bestScore;

    public AlignmentParameter (DefaultParam param){
        this.alignLength = param.alignLength;
        this.bestNas = param.bestNas;
        this.bestKmers = param.bestKmers;
    }
}
