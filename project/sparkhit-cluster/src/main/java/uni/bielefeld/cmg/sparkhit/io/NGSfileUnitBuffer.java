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
 * This is an interface for managing input NGS (next generation sequencing) files
 * via different input buffers.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public interface NGSfileUnitBuffer {
    /**
     * This is an abstract method for buffering input fastq units.
     *
     * @param read {@link readInfo} a type of data structure class describing a sequencing read.
     * @param unitsMark a integer marker counting input fastq units for the buffer.
     */
    void addReadUnit(readInfo read, int unitsMark);
}
