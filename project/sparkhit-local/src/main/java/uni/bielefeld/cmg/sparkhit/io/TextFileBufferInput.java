package uni.bielefeld.cmg.sparkhit.io;


import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
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


public class TextFileBufferInput implements InputFileManager{
    private String inputFile;
    private File inputPath;
    public FileInputStream inputFileStream;
    public BZip2CompressorInputStream inputBZip2Stream;
    public GzipCompressorInputStream inputGzipStream;
    public TarArchiveInputStream inputTarArchiveStream;
    public InputStreamReader inputStreamReader;
    public BufferedReader inputBufferReader;
    public int fileFormatIntMark;

    private InfoDumper info = new InfoDumper();

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
     *
     * @return
     */
    public BufferedReader getBufferReader(){
        return inputBufferReader;
    }

    /**
     *
     * @return
     */
    public InputStreamReader getInputStreamReader(){
        return inputStreamReader;
    }

    /**
     *
     * @return
     */
    public FileInputStream getInputFileStream(){
        return inputFileStream;
    }

    /**
     *
     */
    public void setBufferReader(){
        this.inputBufferReader = new BufferedReader(inputStreamReader);
    }

    /**
     *
     */
    private void setInputStreamReader(){
        if (fileFormatIntMark == 0) {
            this.inputStreamReader = new InputStreamReader(inputFileStream);
        } else if (fileFormatIntMark == 1) {
            setGzipCompressorInputStream();
            this.inputStreamReader = new InputStreamReader(inputGzipStream);
        } else if (fileFormatIntMark == 2) {
            setBZip2CompressorInputStream();
            this.inputStreamReader = new InputStreamReader(inputBZip2Stream);
        } else if (fileFormatIntMark == 3) {
            setGzipCompressorInputStream();
            setTarArchiveInputStreamFromGzipStream();
            try {
                TarArchiveEntry currentEntry = inputTarArchiveStream.getNextTarEntry();
                info.readMessage("Reading the first TarArchive Entry : " + currentEntry.getName());
                info.screenDump();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.inputStreamReader = new InputStreamReader(inputTarArchiveStream);
        } else if (fileFormatIntMark == 4) {
            setBZip2CompressorInputStream();
            setTarArchiveInputStreamFromBZipStream();
            try {
                TarArchiveEntry currentEntry = inputTarArchiveStream.getNextTarEntry();
                info.readMessage("Reading the first TarArchive Entry : " + currentEntry.getName());
                info.screenDump();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.inputStreamReader = new InputStreamReader(inputTarArchiveStream);
        } else {
            info.readMessage("fileFormatIntMark should be 0-4: 0 for uncompressed file; 1 for .gz file; 2 for .bz2 file; 3 for .tar.gz file; 4 for .tar.bz2 file.");
            info.screenDump();
        }
    }

    public void setTarArchiveInputStreamFromGzipStream() {
        this.inputTarArchiveStream = new TarArchiveInputStream(inputGzipStream);
    }

    private void setGzipCompressorInputStream() {
        try {
            this.inputGzipStream = new GzipCompressorInputStream(inputFileStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean setNextTarArchiveEntry() {
        try {
            TarArchiveEntry currentEntry;
            if ((currentEntry = inputTarArchiveStream.getNextTarEntry()) != null) {
                info.readMessage("Reading next TarArchive Entry : " + currentEntry.getName());
                info.screenDump();
                this.inputStreamReader = new InputStreamReader(inputTarArchiveStream);
                setBufferReader();
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void setTarArchiveInputStreamFromBZipStream() {
        this.inputTarArchiveStream = new TarArchiveInputStream(inputBZip2Stream);
    }

    private void setBZip2CompressorInputStream() {
        try {
            this.inputBZip2Stream = new BZip2CompressorInputStream(inputFileStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
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
     *
     */
    private void setInputFile(){
        this.inputPath = new File(inputFile);
    }

    public int checkFile(String cFile) {

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
                System.exit(0);
            } else if (cFile.endsWith("tar.gz")) {
                info.readMessage("Reading fastq file using BufferedReader");
                info.screenDump();
                info.readMessage("Uncompress tar.gz file using GzipCompressor and TarArchive");
                info.screenDump();
                return 3;
            } else if (cFile.endsWith("tar.bz2")) {
                info.readMessage("Reading fastq file using BufferedReader");
                info.screenDump();
                info.readMessage("Uncompress tar.bz2 file using GzipCompressor and TarArchive");
                info.screenDump();
                return 4;
            } else if (cFile.endsWith("gz")) {
                info.readMessage("Reading fastq file using BufferedReader");
                info.screenDump();
                info.readMessage("Uncompress gz file using GzipCompressor");
                info.screenDump();
                return 1;
            } else if (cFile.endsWith("bz2")) {
                info.readMessage("Reading fastq file using BufferedReader");
                info.screenDump();
                info.readMessage("Uncompress bz2 file using BZip2Compressor");
                info.screenDump();
                return 2;
            } else {
                info.readMessage("Reading fastq file using FastqUnitBuffer");
                info.screenDump();
            }
        }

        return 0;
    }

    /**
     *
     * @param inputFile
     */
    public void setInput(String inputFile){
        this.inputFile = inputFile;
        this.fileFormatIntMark = checkFile(inputFile);
        setInputFile();
        setFileInputStream();
        setInputStreamReader();
        setBufferReader();
    }

    /**
     *
     * @param inputFile is the input text file in String
     */
    public void bufferInputFile(String inputFile){
        this.inputFile = inputFile;
    }

    /**
     *
     * @param OutputFile
     */
    public void bufferOutputFile(String OutputFile){
        /**
         * this method is invalid in this class
         */
    }
}
