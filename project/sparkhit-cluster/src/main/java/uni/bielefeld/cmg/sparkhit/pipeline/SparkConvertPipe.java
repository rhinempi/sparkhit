package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.zookeeper.version.Info;
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


public class SparkConvertPipe implements Serializable{
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

        JavaRDD<String> FastqRDD = sc.textFile(param.inputFqPath);

        class FastqFilter implements Function<String, Boolean>, Serializable{
            public Boolean call(String s){
                if (s != null){
 //                   if (s.startsWith("@")){
                        return true;
//                    }else{
//                        return false;
//                    }
                }else{
                    return false;
                }
            }
        }

        class FastqConcat implements Function<String, String>, Serializable{
            String line = "";
            int lineMark = 0;
            public String call(String s){
                if (s.startsWith("@")){
                    line = s;
                    lineMark = 1;
                    return null;
                }else if (lineMark == 1){
                    line = line + "\t" + s;
                    lineMark = 2;
                    return line;
                }else{
                    lineMark = 2;
                    return null;
                }
            }
        }

        class FastqConcatWithQual implements Function<String, String>, Serializable{
            String line = "";
            int lineMark = 0;
            public String call(String s) {
                if (lineMark == 2) {
                    lineMark++;
                    line = line + "\t" + s;
                    return null;
                } else if (lineMark == 3) {
                    lineMark++;
                    line = line + "\t" + s;
                    return line;
                } else if (s.startsWith("@")) {
                    line = s;
                    lineMark = 1;
                    return null;
                } else if (lineMark == 1) {
                    line = line + "\t" + s;
                    lineMark++;
                    return null;
                }else{
                    return null;
                }
            }
        }

        class FastqConcatToFasta implements Function<String, String>, Serializable{
            String line = "";
            int lineMark = 0;
            public String call(String s){
                if (s.startsWith("@")){
                    line = s;
                    lineMark = 1;
                    return null;
                }else if (lineMark == 1){
                    line = ">" + line + "\n" + s;
                    lineMark = 2;
                    return line;
                }else{
                    lineMark = 2;
                    return null;
                }
            }
        }

        if (param.outputformat == 0) {      // fastq to line without quality
            FastqConcat RDDConcat = new FastqConcat();
            FastqRDD = FastqRDD.map(RDDConcat);
        }else if (param.outputformat == 1){ // fastq to line with quality
            FastqConcatWithQual RDDConcatQ = new FastqConcatWithQual();
            FastqRDD = FastqRDD.map(RDDConcatQ);
        }else {                             // fastq to fasta file
            FastqConcatToFasta RDDConcatToFasta = new FastqConcatToFasta();
            FastqRDD = FastqRDD.map(RDDConcatToFasta);
        }

        FastqFilter RDDFilter = new FastqFilter();
        FastqRDD = FastqRDD.filter(RDDFilter);

        long readNumber = FastqRDD.count();

        info.readMessage("Total read number: " + readNumber);
        info.screenDump();

        if (param.partitions != 0) {
            FastqRDD = FastqRDD.repartition(param.partitions);
        }

        FastqRDD.saveAsTextFile(param.outputPath);


    }

    public void setParam(DefaultParam param){
        this.param = param;
    }
}
