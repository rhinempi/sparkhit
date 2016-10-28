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
 * Created by Liren Huang on 17/03/16.
 * <p/>
 * SparkHit
 * <p/>
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 * <p/>
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 * <p/>
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
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

    public void setParam(DefaultParam param){
        this.param = param;
    }
}
