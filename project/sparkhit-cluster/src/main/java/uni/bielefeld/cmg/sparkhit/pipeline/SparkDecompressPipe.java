package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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


public class SparkDecompressPipe implements Serializable{
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

        class LineToFastq implements Function<String, String>, Serializable{
            public String call(String s){
                String[] fourMusketeers = s.split("\\t");
                if (fourMusketeers[3]!=null) {
                    String unit = fourMusketeers[0] + "\n" + fourMusketeers[1] + "\n" + fourMusketeers[2] + "\n" + fourMusketeers[3];
                    return unit;
                }else{
                    return null;
                }
            }
        }

        class RDDUnitFilter implements Function<String, Boolean>, Serializable{
            public Boolean call(String s){
                if (s != null){
                    return true;
                }else{
                    return false;
                }
            }
        }

        class FastqFilterWithQual implements Function<String, String>, Serializable{
            String line = "";
            int lineMark = 0;
            public String call(String s) {
                if (lineMark == 2) {
                    lineMark++;
                    line = line + "\n" + s;
                    return null;
                } else if (lineMark == 3) {
                    lineMark++;
                    line = line + "\n" + s;
                    return line;
                } else if (s.startsWith("@")) {
                    line = s;
                    lineMark = 1;
                    return null;
                } else if (lineMark == 1) {
                    line = line + "\n" + s;
                    lineMark++;
                    return null;
                }else{
                    return null;
                }
            }
        }

        class FastqFilterToFasta implements Function<String, String>, Serializable{
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

        class LineFilterToFasta implements Function<String, String>, Serializable{
            public String call(String s){
                String[] fourMusketeers = s.split("\\t");
                if (fourMusketeers[1]!=null) {
                    String unit = ">" + fourMusketeers[0] + "\n" + fourMusketeers[1];
                    return unit;
                }else{
                    return null;
                }
            }
        }

        if (param.inputFqLinePath != null && param.lineToFasta == false) {      // line to fastq
            LineToFastq RDDLineToFastq = new LineToFastq();
            FastqRDD = FastqRDD.map(RDDLineToFastq);

        }

        if (param.lineToFasta == true){
            LineFilterToFasta RDDLineToFasta = new LineFilterToFasta();
            FastqRDD = FastqRDD.map(RDDLineToFasta);

            RDDUnitFilter RDDFilter = new RDDUnitFilter();
            FastqRDD = FastqRDD.filter(RDDFilter);
        }

        if (param.filterFastq == true){
            FastqFilterWithQual RDDConcatQ = new FastqFilterWithQual();
            FastqRDD = FastqRDD.map(RDDConcatQ);

            RDDUnitFilter RDDFilter = new RDDUnitFilter();
            FastqRDD = FastqRDD.filter(RDDFilter);
        }

        if (param.filterToFasta == true){ // fastq to fasta
            FastqFilterToFasta RDDConcat = new FastqFilterToFasta();
            FastqRDD = FastqRDD.map(RDDConcat);
        }

        if (param.partitions != 0) {
            FastqRDD = FastqRDD.repartition(param.partitions);
        }


        FastqRDD.saveAsTextFile(param.outputPath);


    }

    public void setParam(DefaultParam param){
        this.param = param;
    }
}
