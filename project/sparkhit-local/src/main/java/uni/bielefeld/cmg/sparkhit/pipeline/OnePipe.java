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

    public OnePipe(String threadName){
        this.threadName = threadName;
    }

    /**
     * the pipeline start calls the thread start() function
     */
    public void start(){
        info.readMessage("Start new thread " + threadName);
        info.screenDump();
        if (oneThread == null){
            oneThread = new Thread(this, threadName);
            oneThread.start();
        }
    }

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

    public void run(){
        if (param.readIdentity < 95) {
            try {
                BatchAlignPipe batchPipeline = new BatchAlignPipe();
                batchPipeline.setParam(param);
                batchPipeline.setStruct(ref);
                batchPipeline.setMatrix(matrix);
                while ((read = inputBufferedFastqUnit.nextUnit()) != null) {
                    String outputM = batchPipeline.recruit(read);
                    outputBufferedWriter.write(outputM);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                BatchAlignPipeFast fastBatchPipeline = new BatchAlignPipeFast();
                fastBatchPipeline.setParam(param);
                fastBatchPipeline.setStruct(ref);
                fastBatchPipeline.setMatrix(matrix);
                while ((read = inputBufferedFastqUnit.nextUnit()) != null) {
                    String outputM = fastBatchPipeline.recruit(read);
                    outputBufferedWriter.write(outputM);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void setInputFastqUnitBuffer(FastqUnitBuffer inputBufferedFastqUnit){
        this.inputBufferedFastqUnit = inputBufferedFastqUnit;
    }

    public void setMatrix(ScoreMatrix matrix){
        this.matrix = matrix;
    }

    public void setStruct(RefStructBuilder ref){
        this.ref = ref;
    }

    public void setParameter(DefaultParam param){
        this.param = param;
    }

    public void setInput(BufferedReader InputRead){
        this.inputBufferedReader = InputRead;
    }

    public void setOutput(BufferedWriter OutputWrite){
        this.outputBufferedWriter = OutputWrite;
    }
}
