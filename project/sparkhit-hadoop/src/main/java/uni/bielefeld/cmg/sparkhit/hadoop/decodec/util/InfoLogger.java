package uni.bielefeld.cmg.sparkhit.hadoop.decodec.util;


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
