package uni.bielefeld.cmg.sparkhit.matrix;

import java.io.Serializable;

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

    public ScoreMatrix(){
        initiateMatrix(-6, -1); // not make them program parameters for the moment
    }

    public int[] gapArray = new int[maxGapNum];
    public int[][] matrix = new int[maxNtTypes][maxNtTypes];

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
