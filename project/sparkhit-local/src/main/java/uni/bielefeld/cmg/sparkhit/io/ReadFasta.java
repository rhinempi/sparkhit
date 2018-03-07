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
public class ReadFasta implements InputFileManager {
    private InfoDumper info = new InfoDumper();

    public BufferedReader fasta;

    public ReadFasta(){
        /**
         *
         */
    }

    /**
     *
     * @param inputFastaString
     * @return
     */
    public void createInputFastaStream(String inputFastaString){
        try{
            FileInputStream inputFastaStream = new FileInputStream(inputFastaString);
            InputStreamReader inputFasta = new InputStreamReader(inputFastaStream);
            fasta = new BufferedReader(inputFasta);
        }catch (IOException e) {
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    /**
     *
     * @param cFile
     */
    public void checkFile(String cFile){
        File cFilePath = new File(cFile);

        if (cFile.startsWith("s3")){
            info.readMessage("Input fasta file is located in S3 bucket : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fasta file using Map-Reduce FASTA reader");
            info.screenDump();
        }

        else if (cFile.startsWith("hdfs")) {
            info.readMessage("Input fasta file is lacated in HDFS : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fasta file using hadoop file reader");
            info.screenDump();
        }

        else{
            info.readMessage("Input fasta file is a local file : ");
            info.screenDump();
            info.readMessage("\t" + cFilePath.getAbsolutePath());
            info.screenDump();

            if (!cFilePath.exists()){
                info.readMessage("However, it is not there, please check it again");
                info.screenDump();
                System.exit(0);
            }else {
                info.readMessage("Reading fasta file using BufferedReader");
                info.screenDump();
            }
        }
    }

    /**
     *
     * @return
     */
    public BufferedReader getFastaBufferedReader(){
        return this.fasta;
    }

    /**
     *
     * @param inputFile is the input text file in String
     */
    public void bufferInputFile(String inputFile) {
        checkFile(inputFile);
        createInputFastaStream(inputFile);
    }

    /**
     *
     * @param outputFile
     */
    public void bufferOutputFile(String outputFile){
        /**
         *
         */
    }
}
