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
 * Returns an object for buffering input files. This class is
 * used in local mode only. For cluster mode, Spark "textFile" function
 * is used to access input Fastq file.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class TextFileBufferInput implements InputFileManager{
    private String inputFile;
    private File inputPath;
    public FileInputStream inputFileStream;
    public InputStreamReader inputStreamReader;
    public BufferedReader inputBufferReader;

    private InfoDumper info = new InfoDumper();

    /**
     * A constructor that construct an object of {@link TextFileBufferInput} class.
     * No constructor option needed.
     */
    public TextFileBufferInput(){
        /**
         * Read an input path and set an input buffered reader
         *
         * For synchronized input buffer, send to each thread for
         * parallel reading text file
         *
         */
    }

    /**
     * Returns the preset BufferedReader.
     *
     * @return {@link BufferedReader}.
     */
    public BufferedReader getBufferReader(){
        return inputBufferReader;
    }

    /**
     * Returns the preset InputStreamReade.
     *
     * @return {@link InputStreamReader}.
     */
    public InputStreamReader getInputStreamReader(){
        return inputStreamReader;
    }

    /**
     * Returns the preset FileInputStream.
     *
     * @return {@link FileInputStream}.
     */
    public FileInputStream getInputFileStream(){
        return inputFileStream;
    }

    /**
     * This method sets the BufferedReader based on the preset {@link InputStreamReader}.
     */
    public void setBufferReader(){
        this.inputBufferReader = new BufferedReader(inputStreamReader);
    }

    /**
     * This method sets the InputStreamReader based on the preset {@link FileInputStream}.
     */
    private void setInputStreamReader(){
        this.inputStreamReader = new InputStreamReader(inputFileStream);
    }

    /**
     * This method sets the FileInputStream based on the full path of an input file.
     */
    private void setFileInputStream(){
        try {
            this.inputFileStream = new FileInputStream(inputPath);
        } catch (FileNotFoundException e) {
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    /**
     * This method sets the full path of an input file.
     */
    private void setInputFile(){
        this.inputPath = new File(inputFile);
    }

    /**
     * This method checks the path of an input file. It classifies the
     * location (via URL) of an input file.
     *
     * @param cFile the full
     */
    public void checkFile(String cFile){

        if (cFile.startsWith("s3")){
            info.readMessage("Input file is located in S3 bucket : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading file using Map-Reduce FASTA reader");
            info.screenDump();
        }

        else if (cFile.startsWith("hdfs")) {
            info.readMessage("Input file is lacated in HDFS : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading file using hadoop file reader");
            info.screenDump();
        }

        else{
            info.readMessage("Input file is a local file : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();

            File inputcFile = new File(cFile).getAbsoluteFile();
            if (!inputcFile.exists()){
                info.readMessage("However, it is not there, please check it again");
                info.screenDump();
                throw new RuntimeException("Input file is not in the assigned path.");
            }else {
                info.readMessage("Reading fastq file using FastqUnitBuffer");
                info.screenDump();
            }
        }
    }

    /**
     * This method sets up an input file stream for an input file.
     *
     * @param inputFile the full path of an input file.
     */
    public void setInput(String inputFile){
        this.inputFile = inputFile;
        checkFile(inputFile);
        setInputFile();
        setFileInputStream();
        setInputStreamReader();
        setBufferReader();
    }

    /**
     * This method sets up an input file buffer based on an input file path.
     *
     * @param inputFile is the input text file in String
     */
    public void bufferInputFile(String inputFile){
        this.inputFile = inputFile;
    }

    /**
     * This method sets up an output file buffer based on an output file path.
     *
     * @param OutputFile
     */
    public void bufferOutputFile(String OutputFile){
        /**
         * this method is invalid in this class
         */
    }
}
