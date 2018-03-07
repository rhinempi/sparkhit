package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
 * Returns an object for running the Sparkhit decompression pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
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

        JavaRDD<String> FastqRDD = sc.textFile(param.inputFqPath);

        class LineToFastq implements Function<String, String>, Serializable{
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s a line of the input line-based file.
             * @return the concatenated fastq unit.
             */
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
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the filtering result.
             * @return to be filtered or not.
             */
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

            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s a line of the input fastq file.
             * @return the concatenated fastq unit.
             */
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

            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s a line of the input fastq file.
             * @return the concatenated fasta unit.
             */
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

            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s a line of the input line-based file.
             * @return the concatenated fasta unit.
             */
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
