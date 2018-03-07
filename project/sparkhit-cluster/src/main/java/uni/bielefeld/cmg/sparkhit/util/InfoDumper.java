package uni.bielefeld.cmg.sparkhit.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
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

/**
 * Returns an object for printing messages in a structured format to the screen.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class InfoDumper implements InfoManager, Serializable{

    private String message;
    private String mightyName;

    public InfoDumper (){
        /**
         * format output information and message
         */
        this.mightyName = "SparkHit"; /* the mighty name is your program name which will be used as the header */
    }

    /**
     *
     * @param timeFormat
     * @return
     */
    private String headerMessage(String timeFormat) {
        String currentTime = headerTime(timeFormat);
        String mHeader = mightyName + " " + currentTime;
        return mHeader;
    }

    /**
     * This method formats the message by including the time information.
     *
     * @param timeFormat the raw system time format.
     * @return the formatted time
     */
    private String headerTime(String timeFormat){
        SimpleDateFormat hourMinuteSecond = new SimpleDateFormat(timeFormat);
        String timeHeader = hourMinuteSecond.format(new Date());
        return timeHeader;
    }

    private String paragraphReader(String m){
        String completedParagraph = "";
        String[] paragraphLines = m.split("\n");
        String mHeader = headerMessage("HH:mm:ss");
        for (String line : paragraphLines){
            completedParagraph += mHeader + " " + line + "\n";
        }
        completedParagraph = completedParagraph.trim();
        return completedParagraph;
    }

    /**
     *
     * @param m
     * @return
     */
    private String completeMessage(String m){
        String mHeader = headerMessage("HH:mm:ss");
        String completedMessage = mHeader + " " + m;
        return completedMessage;
    }

    /**
     * This method prints the structured message to the screen.
     */
    public void screenDump(){
        System.out.println(message);
    }

    /**
     * This method records a specified message.
     *
     * @param m the message that is going to be processed.
     */
    public void readMessage(String m) {
        this.message = completeMessage(m);
    }

    /**
     * This method records a paragraph of message.
     *
     * @param m the message that is going to be processed.
     */
    public void readParagraphedMessages(String m ){
        this.message = paragraphReader(m);
    }

    /**
     * This method records an IOException message.
     *
     * @param e the IOException message that is going to be processed.
     */
    public void readIOException(IOException e){
        String m = e.getMessage();
        m = "IOException " + m;
        this.message = completeMessage(m);
    }

    /**
     * This method records a FileNotFoundException message.
     *
     * @param e the FileNotFoundException message that is going to be processed.
     */
    public void readFileNotFoundException(FileNotFoundException e){
        String m = e.getMessage();
        m = "FileNotFoundException " + m;
        this.message = completeMessage(m);
    }

    /**
     * This method records a ClassNotFoundException message.
     *
     * @param e the ClassNotFoundException message that is going to be processed.
     */
    public void readClassNotFoundException(ClassNotFoundException e){
        String m = e.getMessage();
        m = "ClassNotFoundException " + m;
        this.message = completeMessage(m);
    }
}
