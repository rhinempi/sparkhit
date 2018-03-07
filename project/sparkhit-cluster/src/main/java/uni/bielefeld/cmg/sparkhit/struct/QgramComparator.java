package uni.bielefeld.cmg.sparkhit.struct;

import java.io.Serializable;
import java.util.Comparator;

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

/**
 * A comparator class for comparing the q-gram scores of all the q-grams.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class QgramComparator implements Comparator<Qgram>, Serializable {
    /**
     * a compare method that compares the q-gram scores of two q-grams.
     *
     * @param o1 the first q-gram.
     * @param o2 the seconde q-gram.
     * @return the result of comparison.
     */
    public int compare(Qgram o1, Qgram o2){
        if (o1.qGrams == o2.qGrams){
            return 0;
        }
        return o1.qGrams > o2.qGrams ? -1 : 1;
    }
}
