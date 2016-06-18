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
 * Created by rhinempi on 23/01/16.
 *
 *      SparHhit
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
