package uni.bielefeld.cmg.sparkhit.algorithm;

/**
 * Created by Liren Huang on 13/01/16.
 *
 * SparkHit
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
