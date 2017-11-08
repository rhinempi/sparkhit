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

/**
 * Returns an object for buffering output files. This class is
 * used in local mode only. For cluster mode, Spark "textFile" function
 * is used to access input Fastq file.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class TextFileBufferOutput implements OutputFileManager {
    public String path;
    public File outputPath;
    public FileOutputStream outputFileStream;
    public OutputStreamWriter outputStreamWriter;
    public BufferedWriter outputBufferWriter;

    /**
     * A constructor that construct an object of {@link TextFileBufferOutput} class.
     * No constructor option needed.
     */
    public TextFileBufferOutput(){
        /**
         *
         */
    }

    /**
     * Returns the preset BufferedWriter.
     *
     * @return return {@link BufferedWriter} for output text.
     */
    public BufferedWriter getOutputBufferWriter(){
        return outputBufferWriter;
    }

    /**
     * Returns the preset OutputStreamWriter.
     *
     * @return {@link OutputStreamWriter}.
     */
    public OutputStreamWriter getOutputStreamWriter(){
        return outputStreamWriter;
    }

    /**
     * Returns the preset FileOutputStream.
     *
     * @return {@link FileOutputStream}.
     */
    public FileOutputStream getOutputFileStream(){
        return outputFileStream;
    }

    /**
     *
     * set class {@link BufferedWriter} object outputBufferWriter.
     */
    public void setBufferWriter(){
        this.outputBufferWriter = new BufferedWriter(outputStreamWriter);
    }

    /**
     *
     * set class {@link OutputStreamWriter} object outputStreamWriter.
     */
    public void setOutputStreamWriter(){
        this.outputStreamWriter = new OutputStreamWriter(outputFileStream);
    }

    /**
     * set class {@link FileOutputStream} object outputFileStream.
     *
     * @param overwrite stands for whether overwrite the output file or not.
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
     * This method sets the full path of an output file.
     *
     * @param overwrite whether to overwrite the existing file or not.
     */
    private void setOutputFile(boolean overwrite){
        this.outputPath = new File(path).getAbsoluteFile();
    }

    /**
     * This method sets up an output file stream for an output file.
     *
     * @param outputFile the full path of an output file.
     * @param overwrite  whether to overwrite the existing file or not.
     */
    public void setOutput(String outputFile, boolean overwrite){
        this.path = outputFile;
        setOutputFile(overwrite);
        setFileOutputStream(overwrite);
        setOutputStreamWriter();
        setBufferWriter();
    }

    /**
     * This method sets up an output file buffer based on an output file path.
     *
     * @param outputFile the full path of an output file.
     */
    public void bufferOutputFile(String outputFile){
        this.path = outputFile;
    }

    /**
     * This method sets up an input file buffer based on an input file path.
     *
     * @param inputFile the full path of an input file.
     */
    public void bufferInputFile(String inputFile){
        /**
         *   this extended method is invalid
         */
    }
}
