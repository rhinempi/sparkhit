package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferInput;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
 * Returns an object for running the Sparkhit piper pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkParallelPipe implements Serializable{
    private DefaultParam param;
    private InfoDumper info = new InfoDumper();
    List<Integer> data;

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); // default spark context setting
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.sparkhit.serializer.SparkKryoRegistrator");

        return conf;
    }

    private Integer[] loop(int partition) {
        Integer[] a = new Integer[partition];
        for (int i = 0; i < partition; ++i) {
            a[i] = i;
        }
        return a;
    }

    private void createRDDElement(){
        Integer[] a = loop(param.inputNodes);
        data = Arrays.asList(a);
    }

    /**
     * runs the Sparkhit pipeline using Spark RDD operations.
     */
    public void spark() {
        createRDDElement();

        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> distRDD = sc.parallelize(data, param.inputNodes);

        String command = param.toolDepend + " " + param.tool + " " + param.toolParam ;
        info.readMessage(command);
        info.screenDump();

        JavaRDD<String> vcfRDD = distRDD.pipe(command);

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
