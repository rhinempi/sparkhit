package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import scala.Tuple2;
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
 * Returns an object for running the Sparkhit regression pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkRegressionPipe implements Serializable{
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

        JavaRDD<String> vcfRDD1 = sc.textFile(param.inputTrainPath);

        JavaRDD<String> vcfRDD2 = sc.textFile(param.inputFqPath);

        class VariantToLabeledPoint implements Function<String, LabeledPoint> {

            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the VCF file.
             * @return {@link LabeledPoint}.
             */
            public LabeledPoint call(String s) {

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

                LabeledPoint points = new LabeledPoint(1.0, Vectors.dense(vector));

                return points;
            }
        }

        class Filter implements Function<LabeledPoint, Boolean>, Serializable{
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the regression result.
             * @return to be filtered or not.
             */
            public Boolean call(LabeledPoint s){
                if (s != null){
                    return true;
                }else{
                    return false;
                }
            }
        }

        if (param.partitions != 0) {
            vcfRDD2 = vcfRDD2.repartition(param.partitions);
        }

        VariantToLabeledPoint toVector = new VariantToLabeledPoint();
        JavaRDD<LabeledPoint> trainRDD = vcfRDD1.map(toVector);
        JavaRDD<LabeledPoint> testRDD = vcfRDD2.map(toVector);


        Filter RDDFilter = new Filter();
        trainRDD = trainRDD.filter(RDDFilter);
        testRDD = testRDD.filter(RDDFilter);

        if (param.cache) {
            trainRDD.cache();
        }

        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(trainRDD.rdd());

        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testRDD.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );

        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double precision = metrics.precision();
        System.out.println("Precision = " + precision);
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
