package uni.bielefeld.cmg.sparkhit.io;

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
public class TextFileBufferOutput implements OutputFileManager {
    public String path;
    public File outputPath;
    public FileOutputStream outputFileStream;
    public OutputStreamWriter outputStreamWriter;
    public BufferedWriter outputBufferWriter;

    public TextFileBufferOutput(){
        /**
         *
         */
    }

    /**
     *
     * @return return BufferWriter for output text
     */
    public BufferedWriter getOutputBufferWriter(){
        return outputBufferWriter;
    }

    /**
     *
     * @return
     */
    public OutputStreamWriter getOutputStreamWriter(){
        return outputStreamWriter;
    }

    /**
     *
     * @return
     */
    public FileOutputStream getOutputFileStream(){
        return outputFileStream;
    }

    /**
     *
     * set class BufferWriter object outputBufferWriter
     */
    public void setBufferWriter(){
        this.outputBufferWriter = new BufferedWriter(outputStreamWriter);
    }

    /**
     *
     * set class OutputStreamWriter object outputStreamWriter
     */
    public void setOutputStreamWriter(){
        this.outputStreamWriter = new OutputStreamWriter(outputFileStream);
    }

    /**
     * set class FileOutputStream object outputFileStream
     *
     * @param overwrite stands for whether overwrite the log file or not
     */
    public void setFileOutputStream(boolean overwrite){
        try{
            this.outputFileStream = new FileOutputStream(outputPath, overwrite);
        }catch(FileNotFoundException e){
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    /**
     * set class File object outputPath
     *
     * @param overwrite
     */
    private void setOutputFile(boolean overwrite){
        this.outputPath = new File(path).getAbsoluteFile();
    }

    /**
     *
     * @param outputFile out put file path
     * @param overwrite  whether overwrite existing file
     */
    public void setOutput(String outputFile, boolean overwrite){
        this.path = outputFile;
        setOutputFile(overwrite);
        setFileOutputStream(overwrite);
        setOutputStreamWriter();
        setBufferWriter();
    }

    /**
     * Override interface method
     *
     * @param outputFile is the out put file path in String.
     */
    public void bufferOutputFile(String outputFile){
        this.path = outputFile;
    }

    /**
     *
     * @param inputFile
     */
    public void bufferInputFile(String inputFile){
        /**
         *   this extended method is invalid
         */
    }
}
