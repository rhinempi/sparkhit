package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.annotation.meta.param;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferOutput;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

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
 * Returns an object for running the Sparkhit reporter pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkReportPipe implements Serializable{
    private DefaultParam param;
    private InfoDumper info = new InfoDumper();

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
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

        JavaRDD<String> hitsRDD = sc.textFile(param.inputResultPath); // not a fastq file any more

        if (param.partitions != 0) {
            hitsRDD = hitsRDD.repartition(param.partitions);
        }

        class HitsToPairs implements PairFunction<String, String, Integer>{
            /**
             * This function implements the Spark {@link PairFunction}.
             *
             * @param s an input line of the mapping results.
             * @return a key value pair of a hit.
             */
            public Tuple2<String, Integer> call(String s){
                String[] textFq = s.split("\\t");
                int identity = (int) Double.parseDouble(textFq[7]);
                return new Tuple2<String, Integer>(textFq[8] + "\t" + identity, 1);
            }
        }

        class PairsToCount implements Function2<Integer, Integer, Integer>{
            /**
             * This function implements the Spark {@link Function2}.
             *
             * @param i1 the first tuple.
             * @param i2 the second tuple.
             * @return the sum of the values.
             */
            public Integer call(Integer i1, Integer i2){
                return i1 + i2;
            }
        }

        HitsToPairs RDDToPairs = new HitsToPairs();
        JavaPairRDD<String, Integer> hitsPairRDD = hitsRDD.mapToPair(RDDToPairs);

        PairsToCount PairRDDToCount = new PairsToCount();
        JavaPairRDD<String, Integer> countsRDD = hitsPairRDD.reduceByKey(PairRDDToCount);

        JavaPairRDD<String, Integer> countsRDD1 = countsRDD.coalesce(1);
        countsRDD1.saveAsTextFile(param.outputPath);
    }

    /**
     * runs the Sparkhit pipeline using Spark RDD operations.
     */
    public void sparkSpecific() {
        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> hitsRDD = sc.textFile(param.inputResultPath); // not a fastq file any more

        if (param.partitions != 0) {
            hitsRDD = hitsRDD.repartition(param.partitions);
        }

        class HitsToPairs implements PairFunction<String, String, Integer>{
            /**
             * This function implements the Spark {@link PairFunction}.
             *
             * @param s an input line of the mapping results.
             * @return a key value pair of a hit.
             */
            public Tuple2<String, Integer> call(String s){
                String[] textResult = s.split("\\t");
                String[] columnsKey = param.word.split(",");
                int columnsValue = 1;
                if(param.count!=0) {
                    columnsValue = Integer.decode(textResult[param.count-1]);
                }
                String myKey = "";
                for (String column : columnsKey){
                    int index = Integer.decode(column)-1;
                    myKey += textResult[index] + "|";
                }
                return new Tuple2<String, Integer>(myKey, columnsValue);
            }
        }

        class PairsToCount implements Function2<Integer, Integer, Integer>{
            /**
             * This function implements the Spark {@link Function2}.
             *
             * @param i1 the first tuple.
             * @param i2 the second tuple.
             * @return the sum of the values.
             */
            public Integer call(Integer i1, Integer i2){
                return i1 + i2;
            }
        }

        HitsToPairs RDDToPairs = new HitsToPairs();
        JavaPairRDD<String, Integer> hitsPairRDD = hitsRDD.mapToPair(RDDToPairs);

        PairsToCount PairRDDToCount = new PairsToCount();
        JavaPairRDD<String, Integer> countsRDD = hitsPairRDD.reduceByKey(PairRDDToCount);

//        JavaPairRDD<String, Integer> countsRDD1 = countsRDD.coalesce(1);
  //      countsRDD1.saveAsTextFile(param.outputPath);
        /**
         * write into local file
         */
        TextFileBufferOutput reportWriter = new TextFileBufferOutput();
        reportWriter.setOutput(param.outputPath,true);
        BufferedWriter reportBufferedWriter = reportWriter.getOutputBufferWriter();

        List<Tuple2<String, Integer>> reportList = countsRDD.collect();
        for (Tuple2<String, Integer> countTuple : reportList){
            try {
                reportBufferedWriter.write(countTuple._2() + "\t" + countTuple._1() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
                info.readIOException(e);
                info.screenDump();
                System.exit(1);
            }
        }
        try {
            reportBufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

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
