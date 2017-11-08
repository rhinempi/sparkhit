package uni.bielefeld.cmg.sparkhit.matrix;

import java.io.Serializable;

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
 * Returns an instance of data structure class that stores scoring matrix
 * for sequence alignment.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ScoreMatrix implements Serializable, ShMatrix {

    final int maxGapNum = 4096;
    final int maxNtTypes = 6;
    final int[] BLOSUM62 = {
            1,                // A [0]
            -2, 1,            // C [1]
            -2,-2, 1,         // G [2]
            -2,-2,-2, 1,      // T [3]
            -2,-2,-2, 1, 1,   // U [4]
            -2,-2,-2,-2,-2, 1 // N [5]
        //  A  C  G  T  U  N
    };

    /**
     * A constructor that construct an object of {@link ScoreMatrix} class.
     * No constructor option needed.
     */
    public ScoreMatrix(){
        initiateMatrix(-6, -1); // not make them program parameters for the moment
    }

    public int[] gapArray = new int[maxGapNum];
    public int[][] matrix = new int[maxNtTypes][maxNtTypes];

    /**
     * This method initiates the scoring matrix based on the gap penalty
     * and gap extension penalty.
     *
     * @param gap the penalty score for the first nucleotide in a gap.
     * @param extendGap the penalty score for extra nucleotides in the gap.
     */
    public void initiateMatrix(int gap, int extendGap){
        for(int i = 0; i<maxGapNum; i++){
            gapArray[i] = gap + i*extendGap;
        }

        int k=0;
        for (int i =0; i< maxNtTypes; i++){
            for(int j=0; j<= i; j++){
                matrix[i][j]=matrix[j][i] = BLOSUM62[k++];
            }
        }
    }
}
