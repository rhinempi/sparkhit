package uni.bielefeld.cmg.sparkhit.struct;

import java.io.Serializable;

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
