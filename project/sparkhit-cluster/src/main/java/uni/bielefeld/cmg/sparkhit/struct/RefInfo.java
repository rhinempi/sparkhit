package uni.bielefeld.cmg.sparkhit.struct;

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
 * A data structure class that stores all parameters for a reference genome.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class RefInfo implements Serializable {
    public String refName;
    public long refSize;

    /**
     * A constructor that construct an object of {@link RefInfo} class.
     */
    public RefInfo(){
        /**
         * a data structure storing reference length
         */
    }

    /**
     * This method sets the id of a reference genome.
     *
     * @param n the id of a reference genome (contig).
     */
    public void name (String n){
        this.refName = n;
    }

    /**
     * This method sets the size of a reference genome.
     *
     * @param s the size of a reference genome.
     */
    public void size (long s){
        this.refSize = s;
    }
}
