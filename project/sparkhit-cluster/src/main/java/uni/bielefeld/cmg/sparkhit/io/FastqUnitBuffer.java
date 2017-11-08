package uni.bielefeld.cmg.sparkhit.io;


import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Created by Liren Huang on 24/01/16.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 */

/**
 * Returns an object for buffering Fastq files. Providing Fastq
 * unit buffer for multi-thread Fastq input stream. This class is used
 * in local mode only. For cluster mode, Spark "textFile" function
 * is used to access input Fastq file.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class FastqUnitBuffer implements NGSfileUnitBuffer{

    private int unitCount = 1000;
    private int pointerInt = unitCount; // make it bigger than unitCount to initiate the first buffer loading
    private int lineCount = 0;
    private InfoDumper info = new InfoDumper();
    public readInfo[] reads;
    public BufferedReader inputBufferedReader;

    /**
     * This method loads 1000 fastq units into buffer for streaming
     * fastq reads. Each fastq unit is a four line string providing
     * essential information for a sequencing unit.
     */
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

            info.readMessage("load " + unitsMark + " fastq read units into buffer");
            info.screenDump();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * A constructor that sets input BufferedReader for buffering input Fastq files.
     *
     * @param inputBufferedReader Java {@link BufferedReader}.
     */
    public FastqUnitBuffer(BufferedReader inputBufferedReader){
        this.inputBufferedReader = inputBufferedReader;
    }

    /**
     * This constructor is deprecated
     *
     * @param inputBufferedReader
     * @param unitCount
     */
    public FastqUnitBuffer(BufferedReader inputBufferedReader, int unitCount){
        this.inputBufferedReader = inputBufferedReader;
        this.unitCount = unitCount;
        this.pointerInt = unitCount;
    }

    /**
     * This method adds a read into the Fastq unit buffer.
     *
     * @param read {@link readInfo} a type of data structure class describing a sequencing read.
     * @param unitsMark the count of buffered Fastq units.
     */
    public void addReadUnit(readInfo read, int unitsMark){
        int unitsMarkIndex = unitsMark -1;
        reads[unitsMarkIndex] = read;
    }

    /**
     * This method provides a thread safe access point for loading Fastq
     * reads.
     *
     * @return {@link readInfo} a type of data structure class describing a sequencing read
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
