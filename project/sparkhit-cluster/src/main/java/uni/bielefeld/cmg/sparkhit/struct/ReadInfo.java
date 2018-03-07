package uni.bielefeld.cmg.sparkhit.struct;

import uni.bielefeld.cmg.sparkhit.io.readInfo;

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
 * A data structure class that stores all parameters for a sequence read.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ReadInfo implements Serializable {
    public String line;
    public String readName;
    public String read;
    public int readSize;

    private readInfo r;

    /**
     * A constructor that construct an object of {@link ReadInfo} class.
     *
     * @param line the nucleotide sequence of a sequencing read.
     */
    public ReadInfo(String line){
        /**
         * a data structure storing NGS reads info
         */
        this.line = line;
        logInfo();
    }

    /**
     * A constructor that construct an object of {@link ReadInfo} class.
     *
     * @param r {@link readInfo}.
     */
    public ReadInfo(readInfo r){
        /**
         * build from a readInfo (different from ReadInfo) object
         */
        this.r = r;
        logInfo2();
    }

    private void logInfo2(){
        name(r.readId);
        seq(r.readSeq);
        size(read);
    }

    /**
     * This method logs all required information from a fastq unit.
     */
    public void logInfo(){
        String[] textFq = line.split("\\t");
        name(textFq[0]);
        seq(textFq[1]);
        size(read);
    }

    /**
     * This method sets the id of a read.
     *
     * @param n the nucleotide sequence of a read.
     */
    public void name(String n){
        this.readName = n;
    }

    /**
     * This method sets the sequence of a read.
     *
     * @param r the nucleotide sequence in a string.
     */
    public void seq(String r){
        this.read = r;
    }

    /**
     * This method sets the length of a sequencing read.
     *
     * @param s the nucleotide sequence of a read.
     */
    public void size(String s){
        this.readSize = s.length();
    }
}
