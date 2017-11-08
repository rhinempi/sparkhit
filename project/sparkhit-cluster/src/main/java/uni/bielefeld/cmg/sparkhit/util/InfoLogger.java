package uni.bielefeld.cmg.sparkhit.util;

import uni.bielefeld.cmg.sparkhit.io.TextFileBufferOutput;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Liren Huang on 13/01/16.
 *
 * SparkHit
 *
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Returns an object for logging messages in a structured format.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class InfoLogger implements InfoManager {
    private String message;
    private File outputPath;

    /**
     *
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

    /**
     * This method formats the message by including the time header.
     *
     * @param m the raw message.
     * @return the formated message.
     */
    private String completeMessage(String m){
        String mHeader = headerMessage("HH:mm:ss");
        String completedMessage = mHeader + m;
        return completedMessage;
    }

    /**
     * This method writes the structured message to a log file.
     *
     * @param logFileWriter a {@link BufferedWriter} of a log file.
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
     * This method parses the output path and sets a BufferedWriter for the output
     *
     * @param path the full path of an output file.
     * @param overwrite whether to overwrite existing file or not.
     * @return
     */
    public BufferedWriter setOutput(String path, boolean overwrite){
        TextFileBufferOutput outputBuffer = new TextFileBufferOutput();
        outputBuffer.setOutput(path, overwrite);
        BufferedWriter logFileWriter = outputBuffer.getOutputBufferWriter();
        return logFileWriter;
    }

    /**
     * This method prints the structured message to the screen.
     */
    public void screenDump (){
        System.out.println(message);
    }

    /**
     * This method records a specified message.
     *
     * @param m the message that is going to be processed.
     */
    public void readMessage (String m){
        this.message = completeMessage(m);
    }

    /**
     * This method records an IOException message.
     *
     * @param e the IOException message that is going to be processed.
     */
    public void readIOException (IOException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }

    /**
     * This method records a FileNotFoundException message.
     *
     * @param e the FileNotFoundException message that is going to be processed.
     */
    public void readFileNotFoundException (FileNotFoundException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }

    /**
     * This method records a ClassNotFoundException message.
     *
     * @param e the ClassNotFoundException message that is going to be processed.
     */
    public void readClassNotFoundException (ClassNotFoundException e){
        String m = e.getMessage();
        this.message = completeMessage(m);
    }
}
