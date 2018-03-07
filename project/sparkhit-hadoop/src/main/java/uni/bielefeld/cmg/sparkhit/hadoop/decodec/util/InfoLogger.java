package uni.bielefeld.cmg.sparkhit.hadoop.decodec.util;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

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
public class InfoLogger implements InfoManager {
    private String message;
    private File outputPath;

    /**
     *
     * @param timeFormat
     * @return
     */
    private String headerMessage(String timeFormat) {
        String mightyName = "SparkHit";
        String currentTime = headerTime(timeFormat);
        String mHeader = mightyName + " " + currentTime;
        return mHeader;
    }

    /**
     *
     * @param timeFormat
     * @return
     */
    private String headerTime(String timeFormat){
        SimpleDateFormat hourMinuteSecond = new SimpleDateFormat(timeFormat);
        String timeHeader = hourMinuteSecond.format(new Date());
        return timeHeader;
    }

    /**
     *
     * @param m
     * @return
     */
    private String completeMessage(String m){
        String mHeader = headerMessage("HH:mm:ss");
        String completedMessage = mHeader + m;
        return completedMessage;
    }

    /**
     *
     * @param logFileWriter
     */
    private void logFileWrite(BufferedWriter logFileWriter){
        try {
            logFileWriter.write(message);
        }catch(IOException e){
            readIOException(e);
            screenDump();
            System.exit(0);
        }
    }

    /**
     *
     * @param path
     * @param overwrite
     * @return
     */
    /*
    public BufferedWriter setOutput(String path, boolean overwrite){
        TextFileBufferOutput outputBuffer = new TextFileBufferOutput();
        outputBuffer.setOutput(path, overwrite);
        BufferedWriter logFileWriter = outputBuffer.getOutputBufferWriter();
        return logFileWriter;
    }
    */

    /**
     *
     */
    public void screenDump (){
        System.out.println(message);
    }

    /**
     *
     * @param m
     */
    public void readMessage (String m){
        this.message = completeMessage(m);
    }

    /**
     *
     * @param e
     */
    public void readIOException (IOException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }

    /**
     *
     * @param e
     */
    public void readFileNotFoundException (FileNotFoundException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }

    /**
     *
     * @param e
     */
    public void readClassNotFoundException (ClassNotFoundException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }
}
