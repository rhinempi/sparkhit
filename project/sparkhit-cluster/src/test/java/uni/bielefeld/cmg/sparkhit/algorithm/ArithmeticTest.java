package uni.bielefeld.cmg.sparkhit.algorithm;

import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.*;

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
public class ArithmeticTest extends TestCase{
    double minor, pairAlign, lambda;
    int readLength, HSP, contigNum, bestScore;
    long totalLength;
    double expectedEValue, delta;

    @Test
    /**
     * test method with the assigned values
     */
    public void testExpectedHSPLength() throws Exception {
        /**
         * all continues methods to generate the E-value.
         */
        HSP = Arithmetic.expectedHSPLength(minor, readLength, totalLength, pairAlign);
        int ereadLength= Arithmetic.effectiveReadLength(readLength, HSP, minor);
        long etotalLength = Arithmetic.effectiveRefLength(totalLength, HSP, contigNum, minor);
        double evalue = Arithmetic.getEValue(bestScore, minor, lambda, ereadLength, etotalLength);

        assertEquals(expectedEValue, evalue, delta);
    }


    /**
     * assigning the values.
     */
    protected void setUp(){
        minor = 0.621;
        pairAlign = 1.12;
        lambda =1.33;
        readLength = 150;
        contigNum = 1;
        totalLength = 140000000;

        bestScore = 147; // perfect match

        expectedEValue = 1.0E-75;

        delta = 1.0; // delta for the e-value distribution
    }
}