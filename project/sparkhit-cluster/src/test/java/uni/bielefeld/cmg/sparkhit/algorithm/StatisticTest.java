package uni.bielefeld.cmg.sparkhit.algorithm;

import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by rhinempi on 22.07.2017.
 *
 *       Sparkhit
 *
 * Copyright (c) 2017.
 *       Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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