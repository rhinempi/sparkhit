package uni.bielefeld.cmg.sparkhit.algorithm;

/**
 * Created by Liren Huang on 13/01/16.
 *
 *     SparkHit
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
