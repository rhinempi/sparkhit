package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.JavaDeserializationStream;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferInput;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
 * Returns an object for reading BAM files from HDFS.
 * This is the main pipeline for accessing BAM files.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkBamPipe implements Serializable{
    private DefaultParam param;
    private InfoDumper info = new InfoDumper();
    private BufferedReader bamListReader;
    private List<String> bamFile = new ArrayList<String>();
    private int slides;

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); // default spark context setting
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.sparkhit.serializer.SparkKryoRegistrator");

        return conf;
    }

    private void LoadBamList(String inputList){
        TextFileBufferInput bamListInput = new TextFileBufferInput();
        bamListInput.setInput(inputList);
        bamListReader = bamListInput.getBufferReader();
    }

    private void createRDDElement(){
        String bam;
        try {
            while ((bam = bamListReader.readLine())!=null){
                bamFile.add(bam);
            }
            slides = bamFile.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * runs the Sparkhit pipeline using Spark RDD operations.
     */
    public void spark() {
        LoadBamList(param.inputList);
        createRDDElement();

        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> bamRDD = sc.parallelize(bamFile,slides);

        String command = param.toolDepend + " " + param.tool + " \"" + param.toolParam + "\"";
        info.readMessage(command);
        info.screenDump();

        JavaRDD<String> vcfRDD = bamRDD.pipe(command);

        vcfRDD.saveAsTextFile(param.outputPath);

        sc.stop();
    }

    /**
     * This method sets the input parameters.
     *
     * @param param {@link DefaultParam}.
     */
    public void setParam(DefaultParam param){
        this.param = param;
    }
}
