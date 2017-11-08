package uni.bielefeld.cmg.sparkhit.matrix;

import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Liren Huang on 02.11.17.
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
public class ScoreMatrixTest extends TestCase {
    int[] Matrix;
    int[] expectedMatrix;

    @Test
    public void testScoreMatrix() throws Exception {
        ScoreMatrix blosum62 = new ScoreMatrix();

        Matrix = blosum62.BLOSUM62;

        assertArrayEquals(expectedMatrix, Matrix);
    }

    protected void setUp(){

        /**
         * The blosum62 scoring matrix for nucleotide sequence alignment
         */
        expectedMatrix = new int[]{1, -2, 1, -2, -2, 1, -2, -2, -2, 1, -2, -2, -2, 1, 1, -2, -2, -2, -2, -2, 1};
    }
}