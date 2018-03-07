package uni.bielefeld.cmg.sparkhit.struct;

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