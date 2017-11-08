package uni.bielefeld.cmg.sparkhit.io;


import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.*;

/**
 * Created by rhinempi on 24/01/16.
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
