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
public class ObjectFileOutput implements OutputFileManager {
    private String path;
    private File outputPath;
    public FileOutputStream outputFileStream;
    public ObjectOutputStream outputObjectStream;
    private InfoDumper info = new InfoDumper();

    public FileOutputStream getFileOutputStream(){
        return outputFileStream;
    }

    public ObjectOutputStream getObjectOutputStream(){
        return outputObjectStream;
    }

    private void setObjectOutputStream(){
        try {
            this.outputObjectStream = new ObjectOutputStream(outputFileStream);
        } catch (IOException e) {
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    private void setFileOutputStream(boolean overwrite){
        try{
            this.outputFileStream = new FileOutputStream(outputPath, overwrite);
        }catch(FileNotFoundException e){
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    private void setOutputFile(){
        this.outputPath = new File(path);
    }

    public void setOutput(String outputFile, boolean overwrite){
        this.path = outputFile;
        setOutputFile();
        setFileOutputStream(overwrite);
        setObjectOutputStream();
    }

    public void bufferOutputFile(String outputFile){
        this.path = outputFile;
    }

    public void bufferInputFile(String inputFile){

    }
}
