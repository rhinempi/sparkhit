package uni.bielefeld.cmg.sparkhit.algorithm;

import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Liren Huang on 02.11.17.
 *
 *      SparkHit
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * FragRec is free software: you can redistribute it and/or modify it
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