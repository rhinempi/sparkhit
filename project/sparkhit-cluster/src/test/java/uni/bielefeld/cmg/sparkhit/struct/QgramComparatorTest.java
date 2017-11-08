package uni.bielefeld.cmg.sparkhit.struct;

import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Liren Huang on 07.11.17.
 *
 *      Sparkhit
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
public class QgramComparatorTest extends TestCase {
    Qgram q1 = new Qgram();
    Qgram q2 = new Qgram();
    int expectedresult;
    int result;

    @Test
    public void testComparator() throws Exception {
        QgramComparator comparator = new QgramComparator();
        result = comparator.compare(q1, q2);

        assertEquals(expectedresult, result);
    }

    /**
     *
     */
    protected void setUp(){
        q1.chr = 1;
        q1.begin =198724;
        q1.end = 198884;
        q1.qGrams = 130;
        q1.bandLeft = 54;
        q1.bandRight = 58;

        q2.chr = 7;
        q2.begin =2326782;
        q2.end = 2326942;
        q2.qGrams = 128;
        q2.bandLeft = 52;
        q2.bandRight = 56;

        expectedresult = -1; // qGram 1 value is larger than qGram 2
    }
}