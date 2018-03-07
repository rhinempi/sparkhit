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

/**
 * Returns an object that can then be used for varies calculations.
 * This class can also be used as static methods, without building
 * an instance for each operations.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class Arithmetic implements ShAlgorithm{

    public Arithmetic(){
        /**
         * Mainly for static methods of different algorithms for e value calculation
         */
    }

    /* expected HSP length */

    /**
     * This method calculates the high-scoring segment pairs (HSP) length.
     *
     * @param minor default minor value
     * @param readLength input length of each sequencing read.
     * @param totalLength the total sequence length of reference genome.
     * @param pairAlign
     * @return an integer: the high-scoring segment pairs (HSP) length.
     */
    public static int expectedHSPLength(double minor, int readLength, long totalLength, double pairAlign ){
        return (int)(Math.log(minor*readLength*totalLength) / pairAlign);
    }

    /* effective read length */

    /**
     * This method calculates the effective read length for E-value test.
     *
     * @param readLength input length of each sequencing read.
     * @param HSP high-scoring segment pairs (HSP) value calculated based on {@link #expectedHSPLength} method.
     * @param minor default minor value.
     * @return an interger: the effective read length.
     */
    public static int effectiveReadLength(int readLength, int HSP, double minor){
        return (readLength-HSP) < 1/minor ? (int)(1/minor) : (readLength-HSP);
    }

    /* effective reference length */

    /**
     * This method calculates the effective reference length for E-value test.
     *
     * @param totalLength input total length of reference genome.
     * @param HSP high-scoring segment pairs (HSP) value calculated based on {@link #expectedHSPLength} method.
     * @param contigNum number of contigs or chromosomes in reference genome file.
     * @param minor default minor value.
     * @return a long: the effective reference length.
     */
    public static long effectiveRefLength(long totalLength, int HSP, int contigNum, double minor){
        int eLengthRef = (int)(totalLength - (HSP*contigNum));
        return eLengthRef < 1/minor ? (int)(1/minor) : eLengthRef;
    }

    /* use above result for e value */

    /**
     * This method calculates the E-value for each alignment.
     *
     * @param raw q-Gram score.
     * @param minor default minor value.
     * @param lambda default lambda value.
     * @param readLength input length of each sequencing read.
     * @param totalLength input total length of reference genome.
     * @return a double: the E-value.
     */
    public static double getEValue(int raw, double minor, double lambda, int readLength, long totalLength){
        return minor*readLength*totalLength*Math.exp(-1*lambda*raw);
    }

}
