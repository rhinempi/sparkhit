package uni.bielefeld.cmg.sparkhit.io;


import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.IOException;

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


public class FastqUnitBuffer implements NGSfileUnitBuffer{

    private int unitCount = 1000;
    private int pointerInt = unitCount; // make it bigger than unitCount to initiate the first buffer loading
    private int lineCount = 0;
    private int unitCountPerKbp = 0;
    private InfoDumper info = new InfoDumper();
    public readInfo[] reads;
    public BufferedReader inputBufferedReader;

    public void loadBufferedFastq(){
        try {
            readInfo read = new readInfo();
            reads = new readInfo[unitCount];
            String line;
            int lineMark =1;
            int unitsMark=0;
            while ( (line = inputBufferedReader.readLine()) != null) {
                lineCount++;
                if (lineMark == 1) {
                    if (!line.startsWith("@")) {
                        info.readMessage("Line \"" + lineCount + "\" does not started with \"@\" as a fastq file unit, skip to the next line.");
                        info.screenDump();
                        continue;
                    } else {
                        read.readId = line;
                        lineMark++;
                        continue;
                    }
                }

                if (lineMark == 2) {
                    read.readSeq = line;
                    lineMark++;
                    continue;
                }

                if (lineMark == 3) {
                    read.readPlus = line;
                    lineMark++;
                    continue;
                }

                if (lineMark == 4) {  // end of unit, add read unit
                    read.readQual = line;
                    unitsMark++;
                    addReadUnit(read, unitsMark);

                    /* buffer loaded */
                    if (unitsMark >= unitCount) {
                        break;
                    }

                    /* reset variables */
                    read = new readInfo();
                    lineMark=1;
                }
            }

            unitCountPerKbp++;
            info.readMessage("load " + unitCountPerKbp * unitsMark + " fastq read units into buffer");
            info.screenDump();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FastqUnitBuffer(BufferedReader inputBufferedReader){
        this.inputBufferedReader = inputBufferedReader;
    }

    public FastqUnitBuffer(BufferedReader inputBufferedReader, int unitCount){
        this.inputBufferedReader = inputBufferedReader;
        this.unitCount = unitCount;
        this.pointerInt = unitCount;
    }

    public void addReadUnit(readInfo read, int unitsMark){
        int unitsMarkIndex = unitsMark -1;
        reads[unitsMarkIndex] = read;
    }

    /**
     * lock unit output
     *
     * @return
     */
    public synchronized readInfo nextUnit(){
        pointerInt++;
        if (pointerInt > unitCount){
            loadBufferedFastq(); // load another batch of buffered unit
            pointerInt = 1;      // reset pointer
        }
        return reads[pointerInt - 1];
    }
}
