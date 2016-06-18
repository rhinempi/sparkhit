package uni.bielefeld.cmg.sparkhit.struct;

import java.io.Serializable;
import java.util.Comparator;

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
public class QgramComparator implements Comparator<Qgram>, Serializable {
    public int compare(Qgram o1, Qgram o2){
        if (o1.qGrams == o2.qGrams){
            return 0;
        }
        return o1.qGrams > o2.qGrams ? -1 : 1;
    }
}
