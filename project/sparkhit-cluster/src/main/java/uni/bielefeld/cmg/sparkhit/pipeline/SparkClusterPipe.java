package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import uni.bielefeld.cmg.sparkhit.algorithm.Statistic;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.Serializable;
import java.util.ArrayList;

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


public class SparkClusterPipe implements Serializable{
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

        JavaRDD<String> vcfRDD = sc.textFile(param.inputFqPath);
        vcfRDD.count();

        class VariantToVector implements Function<String, Vector> {
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

        JavaRDD<Integer> resultRDD;

        if (param.model == 0) {
            BisectingKMeans bkm = new BisectingKMeans().setK(param.clusterNum).setMaxIterations(param.iterationNum);
            BisectingKMeansModel model = bkm.run(vectorRDD);
         //   resultRDD = model.predict(vectorRDD);

            System.out.println("Compute Cost: " + model.computeCost(vectorRDD));
            Vector[] clusterCenters = model.clusterCenters();
            for (int i = 0; i < clusterCenters.length; i++) {
                Vector clusterCenter = clusterCenters[i];
                System.out.println("Cluster Center " + i + ": " + clusterCenter);
            }
        }else {
                KMeansModel clusters = KMeans.train(vectorRDD.rdd(), param.clusterNum, param.iterationNum);
                //    resultRDD = clusters.predict(vectorRDD);

                Vector[] clusterCenters = clusters.clusterCenters();
                for (int i = 0; i < clusterCenters.length; i++) {
                    Vector clusterCenter = clusterCenters[i];
                    System.out.println("Cluster Center " + i + ": " + clusterCenter);
                }
        }

       // resultRDD.saveAsTextFile(param.outputPath);
        sc.stop();

    }

    public void setParam(DefaultParam param){
        this.param = param;
    }
}
