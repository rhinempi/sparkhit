package uni.bielefeld.cmg.sparkhit.pipeline;


import uni.bielefeld.cmg.sparkhit.io.FastqUnitBuffer;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferInput;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferOutput;
import uni.bielefeld.cmg.sparkhit.io.readInfo;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Created by Liren Huang on 23/01/16.
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
 * Returns an object for running fast mode of fragment recruitment. This class is
 * used in local mode only. For cluster mode, Spark RDD is used to parallelize
 * the tasks.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class FastPipe implements Pipeline {
    private DefaultParam param;
    private BufferedReader InputRead;
    private BufferedWriter OutputWrite;

    private InfoDumper info = new InfoDumper();
    private readInfo read = new readInfo();
    private TextFileBufferOutput outputBufferedFile = new TextFileBufferOutput();

    /**
     * This method writes a line of message to the output stream.
     *
     * @param outputM a message to be written.
     */
    public void writeFastqLine(String outputM){
        try {
            OutputWrite.write(outputM);
        } catch (IOException e) {
            info.readIOException(e);
            info.screenDump();
            System.exit(0);
        }
    }

    /**
     * This method changes the format of the sequencing data.
     */
    public void runChangeFormat() {
        TextFileBufferInput inputFileBuffer = new TextFileBufferInput();
        inputFileBuffer.setInput(param.inputFqPath);
        InputRead = inputFileBuffer.getBufferReader();

        FastqUnitBuffer fastqBufferedUnit = new FastqUnitBuffer(InputRead);

        /**
         * set output buffer
         */
        outputBufferedFile.setOutput(param.outputPath, false);
        OutputWrite = outputBufferedFile.getOutputBufferWriter();

        while( (read = fastqBufferedUnit.nextUnit()) != null){
            String outputM = read.readId + "\t" + read.readSeq + "\t" + read.readPlus + "\t" + read.readQual + "\n";
            writeFastqLine(outputM);
        }

        try {
            OutputWrite.close();
        } catch (IOException e) {
            e.printStackTrace();
            info.readIOException(e);
            info.screenDump();
        }
    }


    /**
     * This method sets the parameters.
     *
     * @param param {@link DefaultParam} is the object for command line parameters.
     */
    public void setParameter(DefaultParam param){
        this.param = param;
    }

    /**
     * This method sets the buffer for reading the input data.
     *
     * @param InputRead a {@link BufferedReader} to read input data.
     */
    public void setInput(BufferedReader InputRead){
        this.InputRead = InputRead;
    }

    /**
     * This method sets the buffer for writing the output data.
     *
     * @param OutputWrite a {@link BufferedWriter} to write to an output file.
     */
    public void setOutput(BufferedWriter OutputWrite){
        this.OutputWrite = OutputWrite;
    }
}
