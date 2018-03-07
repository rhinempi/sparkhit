package uni.bielefeld.cmg.sparkhit.algorithm;

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
public class Arithmetic implements ShAlgorithm{
    public Arithmetic(){
        /**
         * Mainly for static methods of different algorithms for e value calculation
         */
    }

    /* expected HSP length */
    public static int expectedHSPLength(double minor, int readLength, long totalLength, double pairAlign ){
        return (int)(Math.log(minor*readLength*totalLength) / pairAlign);
    }

    /* effective read length */
    public static int effectiveReadLength(int readLength, int HSP, double minor){
        return (readLength-HSP) < 1/minor ? (int)(1/minor) : (readLength-HSP);
    }

    /* effective reference length */
    public static long effectiveRefLength(long totalLength, int HSP, int contigNum, double minor){
        int eLengthRef = (int)(totalLength - (HSP*contigNum));
        return eLengthRef < 1/minor ? (int)(1/minor) : eLengthRef;
    }

    /* use above result for e value */
    public static double getEValue(int raw, double minor, double lambda, int readLength, long totalLength){
        return minor*readLength*totalLength*Math.exp(-1*lambda*raw);
    }

}
