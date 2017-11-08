package uni.bielefeld.cmg.sparkhit.struct;

import uni.bielefeld.cmg.sparkhit.util.DefaultParam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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

/**
 * A data structure class that stores all parameters for sequence alignment.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class AlignmentParameter implements Serializable{
    final public int maxReadLength = 500;
    final public int maxBlockLength = 2*maxReadLength +15*3;
    final public int oneNtBits = 4;
    final public int twoNtBits = oneNtBits * oneNtBits;
    final public int threeNtBits = twoNtBits * oneNtBits;
    final public int fourNtBits = threeNtBits * threeNtBits;
    final int maxNxM = (1<<(oneNtBits*4)) -1; // O(n*m) complexity

    public int[] kmers =new int[maxReadLength];
    public int[] revKmers =new int[maxReadLength];

    public int[] bRead =new int[maxReadLength/15+1];
    public int[] bRevRead =new int[maxReadLength/15+1];

    public int[] fourMer =new int[fourNtBits];
    public int[] fourMerBegin =new int[fourNtBits];
    public int[] fourMerLoci =new int[maxNxM];
    public int[] fourMerScore = new int[maxNxM];

    public List<Long> kmerHits = new ArrayList<Long>();
    public List<CandidateBlock> cRefBlockList = new ArrayList<CandidateBlock>();
    public List<CandidateBlock> mergeRefBlockList = new ArrayList<CandidateBlock>();

    public int[] singleNtBit = new int[maxReadLength+15];   // read bit seq
    public int[] singleNtBitComplement = new int[maxReadLength+15];

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

    /**
     * A constructor that construct an object of {@link AlignmentParameter} class.
     *
     * @param param {@link DefaultParam}.
     */
    public AlignmentParameter (DefaultParam param){
        this.alignLength = param.alignLength;
        this.bestNas = param.bestNas;
        this.bestKmers = param.bestKmers;
        this.bestPigeon = param.bestPigeon;
    }
}
