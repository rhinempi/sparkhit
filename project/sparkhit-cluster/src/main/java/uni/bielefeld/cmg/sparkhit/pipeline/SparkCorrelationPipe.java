package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.Serializable;

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
 * Returns an object for running the Sparkhit correlation test pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkCorrelationPipe implements Serializable{
    private DefaultParam param;
    private InfoDumper info = new InfoDumper();

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); // default spark context setting
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.sparkhit.serializer.SparkKryoRegistrator");

        return conf;
    }

    /**
     * runs the Sparkhit pipeline using Spark RDD operations.
     */
    public void spark() {
        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> vcfRDD = sc.textFile(param.inputFqPath);

        class VariantToVector implements Function<String, Vector> {

            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of a VCF file.
             * @return a vector of all variants from the VCF file.
             */
            public Vector call(String s) {

                if (s.startsWith("#")) {
                    return null;
                }

                String[] array = s.split("\\t");
                double[] vector = new double[param.columnEnd - param.columnStart + 1];

                if (array.length < param.columnEnd) {
                    return null;
                }

                for (int i = param.columnStart-1; i < param.columnEnd; i++) {
                    if (array[i].startsWith("0|0")) {
                        vector[i-param.columnStart+1] = 0;
                    } else if (array[i].startsWith("0|1") || array[i].startsWith("1|0")) {
                        vector[i-param.columnStart+1] = 1;
                    } else if (array[i].startsWith("1|1")) {
                        vector[i-param.columnStart+1] = 2;
                    }
                }
                return Vectors.dense(vector);
            }
        }

        class Filter implements Function<Vector, Boolean>, Serializable {

            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the correlation result.
             * @return to be filtered or not.
             */
            public Boolean call(Vector s) {
                if (s != null) {
                    return true;
                } else {
                    return false;
                }
            }
        }

        if (param.partitions != 0) {
            vcfRDD = vcfRDD.repartition(param.partitions);
        }

        VariantToVector toVector = new VariantToVector();
        JavaRDD<Vector> vectorRDD = vcfRDD.map(toVector);

        Filter RDDFilter = new Filter();
        vectorRDD = vectorRDD.filter(RDDFilter);

        if (param.cache){
            vectorRDD.cache();
        }

        Matrix correlMatrix = Statistics.corr(vectorRDD.rdd(), "pearson");
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
