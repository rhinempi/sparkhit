package uni.bielefeld.cmg.sparkhit.algorithm;

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
public class StatisticTest extends TestCase {
    int obs_hets, obs_hom1, obs_hom2;
    String expectedResult;

    @Test
    public void testStatistic() throws Exception {

        String result =Statistic.calculateHWEP(obs_hets, obs_hom1, obs_hom2);
        assertEquals(expectedResult, result);
    }

    protected void setUp(){
        obs_hets=32; // 32 Aa
        obs_hom1=16; // 32 AA
        obs_hom2=16; // 32 aa

        expectedResult = 32.0 + "\t" + 64.0 + "\t" + 32.0;
    }
}