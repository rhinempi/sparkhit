package uni.bielefeld.cmg.sparkhit.hadoop.decodec.util;

import java.io.Serializable;
import java.util.regex.Pattern;

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
public class DefaultParam implements Serializable{

    public DefaultParam (){
        /**
         * This is a class of data structure which stores the default parameters
         */
    }

    public String mightyName = "SparkHit";
    public boolean bz2 = false;
    public boolean gz = false;
    public String inputFqPath;              // undefaultable
    public String outputPath;
    public Integer outputFormat=0;
    public boolean overwrite = false;
    public long splitsize = 0;


}
