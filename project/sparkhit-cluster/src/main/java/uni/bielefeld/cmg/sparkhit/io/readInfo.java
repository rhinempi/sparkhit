package uni.bielefeld.cmg.sparkhit.io;


/**
 * Created by rhinempi on 24/01/16.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 */


/**
 * This is a data structure class. It returns an instance of a data structure
 * that stores a fastq unit that describes a sequencing read.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class readInfo {
    public String readId;   // @Hiseq-2000-AWESOME-MACHINE
    public String readSeq;  // ATCGATCGATCGATCGATCGATCGATCG
    public String readPlus; // +
    public String readQual; // BBBBBBIIIIIIIIBBBIIIBIIBIIBI
}
