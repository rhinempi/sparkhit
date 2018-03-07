package uni.bielefeld.cmg.sparkhit.io;

import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.*;

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
 * Returns an object for buffering input fastq files. This class is
 * used in local mode only. For cluster mode, Spark "textFile" function
 * is used to access input Fastq file.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ReadFastq implements InputFileManager {
    private InfoDumper info = new InfoDumper();
    private readInfo read = new readInfo();
    private String inputFastq;

    public BufferedReader fastq;
    public FastqUnitBuffer BufferedFastqUnit;

    /**
     * A constructor that construct an object of {@link ReadFastq} class.
     * No constructor option needed.
     */
    public ReadFastq(){
        /**
         *  not used here
         */
    }

    /**
     * Use this method to get the buffer for accessing fastq unit.
     *
     * @return {@link FastqUnitBuffer}.
     */
    public FastqUnitBuffer getBufferedFastqUnit(){
        return this.BufferedFastqUnit;
    }

    /**
     * This method loads {@param units} number fastq units into buffer
     * for streaming fastq reads. Each fastq unit is a four line string
     * providing essential information for a sequencing unit.
     *
     * @param units number of input fastq units per batch (buffer size).
     * @return {@link FastqUnitBuffer}.
     */
    public FastqUnitBuffer loadBufferedFastq(int units){
        try {
            String line;
            int lineMark = 0;
            int lineCount = 0;
            int unitsMark = 0;
            BufferedFastqUnit = new FastqUnitBuffer(fastq);
            while ((line = fastq.readLine()) != null) {
                lineCount++;
                if (lineMark == 0){
                    if (!line.startsWith("@")){
                        info.readMessage("Line \"" + lineCount + "\" does not started with \"@\" as a fastq file unit, skip to the next line.");
                        info.screenDump();
                        continue;
                    }else {
                        read.readId = line;
                        lineMark++;
                        continue;
                    }
                }

                if (lineMark == 1){
                    read.readSeq = line;
                    lineMark++;
                    continue;
                }

                if (lineMark == 2){
                    read.readPlus = line;
                    lineMark++;
                    continue;
                }

                if (lineMark == 3){
                    read.readQual = line;
                    BufferedFastqUnit.addReadUnit(read, unitsMark);
                    System.out.println("add one unit");
                    unitsMark++;
                    if (unitsMark >= units){
                        return BufferedFastqUnit;
                    }
                    lineMark = 0;
                }
            }
        }catch(IOException e){
            e.fillInStackTrace();
            System.exit(0);
        }
        return BufferedFastqUnit;
    }

    /**
     * Only when you want to creat bufferedReader with this class.
     * Usually I use TextFileBufferInput to create a synchronized
     * BufferedReader and parallelize getRead in threads.
     *
     * @param inputFastqString full path of an input fastq file.
     * @return null.
     */
    public void createInputFastqStream(String inputFastqString){
        try{
            FileInputStream inputFastqStream = new FileInputStream(inputFastqString);
            InputStreamReader inputFastq = new InputStreamReader(inputFastqStream);
            fastq = new BufferedReader(inputFastq);
        }catch (IOException e) {
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    /**
     *This method checks the path of input fastq file. It classifies the
     * location (via URL) of an input file.
     *
     * @param cFile the full path of an input fastq file.
     */
    public void checkFile(String cFile){

        if (cFile.startsWith("s3")){
            info.readMessage("Input fastq file is located in S3 bucket : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fastq file using Map-Reduce FASTQ reader");
            info.screenDump();
        }

        else if (cFile.startsWith("hdfs")) {
            info.readMessage("Input fastq file is lacated in HDFS : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fastq file using hadoop file reader");
            info.screenDump();
        }

        else{
            info.readMessage("Input fastq file is a local file : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();

            File inputFile = new File(cFile).getAbsoluteFile();
            if (!inputFile.exists()){
                info.readMessage("However, it is not there, please check it again");
                info.screenDump();
                System.exit(0);
            }else {
                info.readMessage("Reading fastq file using BufferedReader");
                info.screenDump();
            }
        }
    }

    /**
     * This method sets the BufferedReader.
     *
     * @param inputBufferedReader {@link BufferedReader}.
     */
    public void setInputBufferedReader(BufferedReader inputBufferedReader){
        this.fastq = inputBufferedReader;
    }

    /**
     * This method sets up an input fastq file stream based on an input file path.
     *
     * @param inputFastq is the input text file in String
     */
    public void bufferInputFile(String inputFastq) {
        this.inputFastq = inputFastq;
    }

    /**
     * This method sets the full path of an output file.
     *
     * @param outputFile is the full path of an output file.
     */
    public void bufferOutputFile(String outputFile){
        /**
         *
         */
    }
}
