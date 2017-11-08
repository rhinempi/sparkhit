package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import uni.bielefeld.cmg.sparkhit.io.FastqUnitBuffer;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferInput;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferOutput;
import uni.bielefeld.cmg.sparkhit.matrix.ScoreMatrix;
import uni.bielefeld.cmg.sparkhit.reference.RefSerializer;
import uni.bielefeld.cmg.sparkhit.reference.RefStructBuilder;
import uni.bielefeld.cmg.sparkhit.reference.RefStructSerializer;
import uni.bielefeld.cmg.sparkhit.struct.BinaryBlock;
import uni.bielefeld.cmg.sparkhit.struct.KmerLoc;
import uni.bielefeld.cmg.sparkhit.struct.RefTitle;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Created by rhinempi on 27/01/16.
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
 * Returns an object for managing different pipelines of each Sparkhit
 * application.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class Pipelines implements Pipeline, Serializable{

    private String threadName;

    private TextFileBufferInput inputFileBuffer;
    private BufferedReader inputBufferedReader;
    private FastqUnitBuffer inputFastqUnitBuffer;

    private TextFileBufferOutput outputFileBuffer;
    private BufferedWriter outputBufferedWriter;

    private DefaultParam param;

    private InfoDumper info = new InfoDumper();

    private OnePipe[] processors;

    private long time;

    private RefStructBuilder ref;

    private ScoreMatrix matrix = new ScoreMatrix();

    private void clockStart(){
        time = System.currentTimeMillis();
    }

    private long clockCut(){
        long tmp = time;
        time = System.currentTimeMillis();
        return time - tmp;
    }

    /**
     * A constructor that construct an object of {@link Pipelines} class.
     */
    public Pipelines () {
    }

    /**
     * This is a test method to test the run time of serializing the reference genomes.
     */
    public void loadReference(){
        RefStructSerializer refSer = new RefStructSerializer();
        refSer.setParameter(param);

        clockStart();
        refSer.kryoDeserialization();
        long T = clockCut();
        info.readParagraphedMessages("loaded reference genome index.\ntook " + T + " ms.");
        info.screenDump();
        ref = refSer.getStruct();
    }

    /**
     * This method builds the reference index.
     */
    public void buildReferenceForSpark(){
        info.readMessage("start building reference index.");
        info.screenDump();
        ref = new RefStructBuilder();

        info.readMessage("parsing parameters ...");
        info.screenDump();
        ref.setParameter(param);
        info.readMessage("kmer size : " + param.kmerSize);
        info.screenDump();

        info.readMessage("start loading reference sequence.");
        info.screenDump();
        clockStart();
        ref.loadRef(param.inputFaPath);
        long T = clockCut();
        info.readParagraphedMessages("loaded " + ref.totalLength + " bases, " + ref.totalNum + " contigs.\ntook " + T + " ms.");
        info.screenDump();

        info.readMessage("start building reference index.");
        info.screenDump();
        clockStart();
        ref.buildIndex();
        T = clockCut();
        info.readMessage("took " + T + " ms.");
        info.screenDump();
    }

    /**
     * This method deserializes the reference index using default Java serializer.
     */
    public void buildJavaReference(){
        info.readMessage("start building reference index.");
        info.screenDump();
        RefStructBuilder ref = new RefStructBuilder();

        info.readMessage("parsing parameters ...");
        info.screenDump();
        ref.setParameter(param);
        info.readParagraphedMessages("kmer size : " + param.kmerSize + "\nout put index file to : \n\t" + param.inputFaPath + ".xxx");
        info.screenDump();

        info.readMessage("start loading reference sequence.");
        info.screenDump();
        clockStart();
        ref.loadRef(param.inputFaPath);
        long T = clockCut();
        info.readParagraphedMessages("loaded " + ref.totalLength + " bases, " + ref.totalNum + " contigs.\ntook " + T + " ms.");
        info.screenDump();

        info.readMessage("start building reference index.");
        info.screenDump();
        clockStart();
        ref.buildIndex();
        T = clockCut();
        info.readMessage("took " + T + " ms.");
        info.screenDump();

        RefStructSerializer refSer = new RefStructSerializer();
        refSer.setParameter(param);
        refSer.setStruct(ref);
        info.readParagraphedMessages("start writing index to : \n\t" + param.inputFaPath + ".xxx");
        info.screenDump();
        clockStart();
        refSer.javaSerialization();
        T = clockCut();
        info.readMessage("took " + T + " ms");
        info.screenDump();

        info.readMessage("finish building reference index.");
        info.screenDump();
    }

    /**
     * This method deserializes the reference index using kryo serializer.
     */
    public void buildReference(){

        info.readMessage("start building reference index.");
        info.screenDump();
        RefStructBuilder ref = new RefStructBuilder();

        info.readMessage("parsing parameters ...");
        info.screenDump();
        ref.setParameter(param);
        info.readParagraphedMessages("kmer size : " + param.kmerSize + "\nout put index file to : \n\t" + param.inputFaPath + ".xxx");
        info.screenDump();

        info.readMessage("start loading reference sequence.");
        info.screenDump();
        clockStart();
        ref.loadRef(param.inputFaPath);
        long T = clockCut();
        info.readParagraphedMessages("loaded " + ref.totalLength + " bases, " + ref.totalNum + " contigs.\ntook " + T + " ms.");
        info.screenDump();

        info.readMessage("start building reference index.");
        info.screenDump();
        clockStart();
        ref.buildIndex();
        T = clockCut();
        info.readMessage("took " + T + " ms.");
        info.screenDump();

        RefStructSerializer refSer = new RefStructSerializer();
        refSer.setParameter(param);
        refSer.setStruct(ref);
        info.readParagraphedMessages("start writing index to : \n\t" + param.inputFaPath + ".xxx");
        info.screenDump();
        clockStart();
        refSer.kryoSerialization();
        T = clockCut();
        info.readMessage("took " + T + " ms");
        info.screenDump();

        info.readMessage("finish building reference index.");
        info.screenDump();
    }

    /**
     * This method starts parallel processes with a selected number of cores.
     * It is only used in local mode. For cluster mode, Sparkhit uses Spark
     * RDD to parallelize tasks.
     *
     * @param cores the number of threads to be created.
     */
    public void parallelization(int cores) {
        inputFileBuffer = new TextFileBufferInput();
        inputFileBuffer.setInput(param.inputFqPath);
        inputBufferedReader = inputFileBuffer.getBufferReader();

        inputFastqUnitBuffer = new FastqUnitBuffer(inputBufferedReader);

        outputFileBuffer = new TextFileBufferOutput();
        outputFileBuffer.setOutput(param.outputPath, false);
        outputBufferedWriter = outputFileBuffer.getOutputBufferWriter();

        processors = new OnePipe[cores];

        info.readMessage("Main pipeline is now alive, start parallelizing.");
        info.screenDump();

        for (int i = 0; i < cores; i++) {
            threadName = "processor P" + i;
            newThread(i);
        }

        for (int i = 0; i < cores; i++) {
            processors[i].join();
        }

        try {
            outputBufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        info.readMessage("Main pipeline finished.");
        info.screenDump();
    }

    /**
     * This method creates a new thread to run a new pipeline. It is only used in
     * local mode. For cluster mode, Sparkhit uses Spark RDD to parallelize
     * tasks.
     *
     * @param pipeIndex the index of this thread.
     */
    public void newThread(int pipeIndex){
        OnePipe pipe = new OnePipe(threadName);
        pipe.setParameter(param);
        pipe.setStruct(ref);
        pipe.setMatrix(matrix);
        pipe.setInputFastqUnitBuffer(inputFastqUnitBuffer);
        pipe.setOutput(outputBufferedWriter);
        pipe.start();
        processors[pipeIndex] = pipe;
    }


    /**
     * This method starts the recruiter pipeline
     */
    public void spark(){
        SparkPipe sPipe = new SparkPipe();
        sPipe.setParam(param);
        if (param.inputFqLinePath != null){
            sPipe.sparkLineFile();
        }else{
            sPipe.spark();
        }
    }

    /**
     * This method starts the mapper pipeline
     */
    public void sparkMapper(){
        SparkPipe sPipe = new SparkPipe();
        sPipe.setParam(param);
        if (param.inputFqLinePath != null){
            sPipe.sparkLineFile();
        }else{
            sPipe.spark();
        }
    }


    /**
     * This method starts the reporter pipeline
     */
    public void sparkReporter(){
        SparkReportPipe sRPipe = new SparkReportPipe();
        sRPipe.setParam(param);
        if (param.word == null) {
            sRPipe.spark();
        }else{
            sRPipe.sparkSpecific();
        }
    }

    /**
     * This method starts the converter pipeline
     */
    public void sparkConverter(){
        SparkConvertPipe sCPipe = new SparkConvertPipe();
        sCPipe.setParam(param);
        sCPipe.spark();
    }

    /**
     * This method starts the piper pipeline
     */
    public void sparkPiper(){
        SparkScriptPipe sPPipe = new SparkScriptPipe();
        sPPipe.setParam(param);
        sPPipe.spark();
    }

    /**
     * This method starts the variant detection pipeline
     */
    public void sparkVariantCaller(){
        SparkBamPipe sBPipe = new SparkBamPipe();
        sBPipe.setParam(param);
        sBPipe.spark();
    }

    /**
     * This method starts the parallelizer pipeline
     */
    public void sparkParallelizer(){
        SparkParallelPipe sPPipe = new SparkParallelPipe();
        sPPipe.setParam(param);
        sPPipe.spark();
    }


    /**
     * This method starts the reduction pipeline
     */
    public void sparkReductioner(){
        SparkReductionPipe sRPipe = new SparkReductionPipe();
        sRPipe.setParam(param);
        sRPipe.spark();
    }

    /**
     * This method starts the HWE test pipeline
     */
    public void sparkStatisticer(){
        SparkHWEPipe sHPipe = new SparkHWEPipe();
        sHPipe.setParam(param);
        sHPipe.spark();
    }

    /**
     * This method starts the decompression pipeline
     */
    public void sparkDecompresser(){
        SparkDecompressPipe sDPipe = new SparkDecompressPipe();
        sDPipe.setParam(param);
        sDPipe.spark();
    }

    /**
     * This method starts the cluster pipeline
     */
    public void sparkCluster(){
        SparkClusterPipe sCPipe = new SparkClusterPipe();
        sCPipe.setParam(param);
        sCPipe.spark();
    }

    /**
     * This method starts the correlation pipeline
     */
    public void sparkCorrelationer(){
        SparkCorrelationPipe sCPipe = new SparkCorrelationPipe();
        sCPipe.setParam(param);
        sCPipe.spark();
    }

    /**
     * This method starts the chi-square test pipeline
     */
    public void sparkChisquareTester(){
        SparkChiSquarePipe sCPipe = new SparkChiSquarePipe();
        sCPipe.setParam(param);
        sCPipe.spark();
    }

    /**
     * This method starts the regression pipeline
     */
    public void sparkRegressioner(){
        SparkReductionPipe sRPipe = new SparkReductionPipe();
        sRPipe.setParam(param);
        sRPipe.spark();
    }

    /**
     * This method sets a buffer for loading fastq units.
     *
     * @param inputFastqUnitBuffer a buffer for reading fastq units.
     */
    public void setFastqUnitBuffer(FastqUnitBuffer inputFastqUnitBuffer){
        this.inputFastqUnitBuffer = inputFastqUnitBuffer;
    }

    /**
     * This method sets correspond parameters.
     *
     * @param param {@link DefaultParam} is the object for command line parameters.
     */
    public void setParameter(DefaultParam param){
        this.param = param;
    }

    /**
     * This method sets input buffer reader.
     *
     * @param inputBufferedReader a {@link BufferedReader} to read input data.
     */
    public void setInput(BufferedReader inputBufferedReader){

    }

    /**
     * This method sets output buffer writer.
     *
     * @param outputBufferedWriter a {@link BufferedWriter} to write to an output file.
     */
    public void setOutput(BufferedWriter outputBufferedWriter){
        this.outputBufferedWriter = outputBufferedWriter;
    }
}
