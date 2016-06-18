package uni.bielefeld.cmg.sparkhit.io;

import java.io.*;

/**
 * Created by Liren Huang on 13/01/16.
 *
 *      SparkHit
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
