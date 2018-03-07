package uni.bielefeld.cmg.sparkhit.pipeline;


import uni.bielefeld.cmg.sparkhit.io.FastqUnitBuffer;
import uni.bielefeld.cmg.sparkhit.io.readInfo;
import uni.bielefeld.cmg.sparkhit.matrix.ScoreMatrix;
import uni.bielefeld.cmg.sparkhit.reference.RefStructBuilder;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

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
 * Returns an object for managing one thread of the pipeline. This class is
 * used in local mode only. For cluster mode, Spark RDD is used to
 * parallelize the tasks.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class OnePipe implements Pipeline,Runnable {
    public DefaultParam param;
    public BufferedReader inputBufferedReader;
    public BufferedWriter outputBufferedWriter;
    public FastqUnitBuffer inputBufferedFastqUnit;

    private Thread oneThread;
    private String threadName;

    private InfoDumper info = new InfoDumper();

    private RefStructBuilder ref;
    private ScoreMatrix matrix;

    public readInfo read;

    /**
     * A constructor that construct an object of {@link OnePipe} class.
     *
     * @param threadName the index of this thread.
     */
    public OnePipe(String threadName){
        this.threadName = threadName;
    }

    /**
     * the pipeline starts by calling the thread start() function
     */
    public void start(){
        info.readMessage("Start new thread " + threadName);
        info.screenDump();
        if (oneThread == null){
            oneThread = new Thread(this, threadName);
            oneThread.start();
        }
    }

    /**
     * wait util all threads finish.
     */
    public void join(){
        if (oneThread != null){
            while(oneThread.isAlive()){
                try {
                    oneThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        info.readMessage(threadName + " finished");
        info.screenDump();
    }

    /**
     * This method runs the fragment recruitment pipeline within this thread.
     */
    public void run(){
        try {
            BatchAlignPipe batchPipeline = new BatchAlignPipe();
            batchPipeline.setParam(param);
            batchPipeline.setStruct(ref);
            batchPipeline.setMatrix(matrix);
            while ((read = inputBufferedFastqUnit.nextUnit()) != null) {
                String outputM = batchPipeline.recruit(read);
                outputBufferedWriter.write(outputM);
            }
        } catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * This method sets the buffer for loading fastq units.
     *
     * @param inputBufferedFastqUnit
     */
    public void setInputFastqUnitBuffer(FastqUnitBuffer inputBufferedFastqUnit){
        this.inputBufferedFastqUnit = inputBufferedFastqUnit;
    }

    /**
     * This method sets the scoring matrix for sequence alignment.
     *
     * @param matrix {@link ScoreMatrix}.
     */
    public void setMatrix(ScoreMatrix matrix){
        this.matrix = matrix;
    }

    /**
     * This method sets the reference index.
     *
     * @param ref {@link RefStructBuilder} the reference index.
     */
    public void setStruct(RefStructBuilder ref){
        this.ref = ref;
    }

    /**
     * This method sets the input parameters.
     *
     * @param param {@link DefaultParam} is the object for command line parameters.
     */
    public void setParameter(DefaultParam param){
        this.param = param;
    }

    /**
     * This method sets the buffer for loading input data.
     *
     * @param InputRead a {@link BufferedReader} to read input data.
     */
    public void setInput(BufferedReader InputRead){
        this.inputBufferedReader = InputRead;
    }

    /**
     * This method sets the buffer for writing output data.
     *
     * @param OutputWrite a {@link BufferedWriter} to write to an output file.
     */
    public void setOutput(BufferedWriter OutputWrite){
        this.outputBufferedWriter = OutputWrite;
    }
}
