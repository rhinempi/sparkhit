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
public class ReadInfo implements Serializable {
    public String line;
    public String readName;
    public String read;
    public int readSize;

    private readInfo r;

    /**
     *
     * @param line
     */
    public ReadInfo(String line){
        /**
         * a data structure storing NGS reads info
         */
        this.line = line;
        logInfo();
    }

    /**
     *
     * @param r
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

    public void logInfo(){
        String[] textFq = line.split("\t");
        name(textFq[0]);
        seq(textFq[1]);
        size(read);
    }

    public void name(String n){
        this.readName = n;
    }

    public void seq(String r){
        this.read = r;
    }

    public void size(String s){
        this.readSize = s.length();
    }
}
