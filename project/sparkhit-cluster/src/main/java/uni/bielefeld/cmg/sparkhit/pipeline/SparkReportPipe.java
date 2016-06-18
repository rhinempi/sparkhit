package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.annotation.meta.param;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

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


public class SparkReportPipe implements Serializable{
    private DefaultParam param;
    private InfoDumper info = new InfoDumper();

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
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

        JavaRDD<String> hitsRDD = sc.textFile(param.inputResultPath); // not a fastq file any more

        if (param.partitions != 0) {
            hitsRDD = hitsRDD.repartition(param.partitions);
        }

        class HitsToPairs implements PairFunction<String, String, Integer>{
            public Tuple2<String, Integer> call(String s){
                String[] textFq = s.split("\\t");
                int identity = (int) Double.parseDouble(textFq[7]);
                return new Tuple2<String, Integer>(textFq[8] + "\t" + identity, 1);
            }
        }

        class PairsToCount implements Function2<Integer, Integer, Integer>{
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

    public void setParam(DefaultParam param){
        this.param = param;
    }
}
