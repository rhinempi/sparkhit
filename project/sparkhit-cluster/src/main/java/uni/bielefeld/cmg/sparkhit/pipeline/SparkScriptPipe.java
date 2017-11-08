package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by Liren Huang on 17/03/16.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 */

/**
 * Returns an object for running the Sparkhit piper pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkScriptPipe implements Serializable{
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

        JavaRDD<String> FastqRDD;

        class LineToFastq implements Function<String, String>, Serializable{

            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input of a fastq unit in a line.
             * @return a fastq unit.
             */
            public String call(String s){
                String[] fourMusketeers = s.split("\\t");
                if (fourMusketeers[3]!=null) {
                    if (fourMusketeers[0].startsWith("@")) {
                        if (fourMusketeers[1].length() == fourMusketeers[3].length()) {
                            String unit = fourMusketeers[0] + "\n" + fourMusketeers[1] + "\n" + fourMusketeers[2] + "\n" + fourMusketeers[3];
                            return unit;
                        }
                    }
                }
                return null;
            }
        }

        class RDDUnitFilter implements Function<String, Boolean>, Serializable{
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the external tool`s result.
             * @return to be filtered or not.
             */
            public Boolean call(String s){
                if (s != null) {
                    return true;
                }
                else {
                    return false;
                }
            }
        }

        class FastqUnitFilter implements Function<String, Boolean>, Serializable{
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input fastq unit.
             * @return to be filtered or not.
             */
            public Boolean call(String s){
                if (s.startsWith("@")){
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
             * @param s an input line of the fastq file.
             * @return a fastq unit.
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
             * @param s an input line of the fastq file.
             * @return a fasta unit.
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
             * @param s an fastq unit of the line-based file.
             * @return a fasta unit.
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

        if (param.filename){
            class LineTuple2String implements Function<Tuple2<String, String>, String>, Serializable{
                /**
                 * This function implements the Spark {@link Function}.
                 *
                 * @param s a fastq header with filename.
                 * @return a concatenated fastq header.
                 */
                public String call(Tuple2<String, String> s){
                    if (s._2.startsWith("@")) {
                        return ("@" + s._1 + "|" + s._2);
                    }else{
                        return s._2;
                    }
                }
            }

            class FastqTuple2String implements Function<Tuple2<String, String>, String>, Serializable{
                String line = "";
                int lineMark = 0;

                /**
                 * This function implements the Spark {@link Function}.
                 *
                 * @param s a fastq header with filename.
                 * @return a fastq unit.
                 */
                public String call(Tuple2<String, String> s){
                    if (lineMark == 2) {
                        lineMark++;
                        line = line + "\n" + s._2;
                        return null;
                    } else if (lineMark == 3) {
                        lineMark++;
                        line = line + "\n" + s._2;
                        return line;
                    } else if (s._2.startsWith("@")) {
                        line = s._2;
                        lineMark = 1;
                        return null;
                    } else if (lineMark == 1) {
                        line = "@" + s._1 + "|" + line + "\n" + s._2; // tag filename when matches ID
                        lineMark++;
                        return null;
                    }else{
                        return null;
                    }
                }
            }

            class RDDTaggedUnitFilter implements Function<String, Boolean>, Serializable{
                /**
                 * This function implements the Spark {@link Function}.
                 *
                 * @param s an input line of the external tool`s result.
                 * @return to be filtered or not.
                 */
                public Boolean call(String s){
                    if (s != null) {
                        return true;
                    }
                    else {
                        return false;
                    }
                }
            }



            JavaPairRDD<LongWritable, Text> javaPairRDD = sc.newAPIHadoopFile(
                    param.inputFqPath,
                    TextInputFormat.class,
                    LongWritable.class,
                    Text.class,
                    new Configuration()
            );

            JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD) javaPairRDD;

            JavaRDD<Tuple2<String, String>> namedLinesRDD = hadoopRDD.mapPartitionsWithInputSplit(
                    new Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, String>>>() {
                        @Override
                        public Iterator<Tuple2<String, String>> call(InputSplit inputSplit, final Iterator<Tuple2<LongWritable, Text>> lines) throws Exception {
                            FileSplit fileSplit = (FileSplit) inputSplit;
                            final String fileName = fileSplit.getPath().getName();
                            return new Iterator<Tuple2<String, String>>() {
                                @Override
                                public boolean hasNext() {
                                    return lines.hasNext();
                                }
                                @Override
                                public Tuple2<String, String> next() {
                                    Tuple2<LongWritable, Text> entry = lines.next();
                                    return new Tuple2<String, String>(fileName, entry._2().toString());
                                }

                                @Override
                                public void remove() {
                                    throw new IllegalStateException();
                                }
                            };
                        }
                    },
                    true
            );

            if (param.inputFqLinePath != null) {
                LineTuple2String RDDmerge = new LineTuple2String();
                FastqRDD = namedLinesRDD.map(RDDmerge);

                RDDTaggedUnitFilter RDDTagFilter = new RDDTaggedUnitFilter();
                FastqRDD = FastqRDD.filter(RDDTagFilter);
            }else{
                FastqTuple2String RDDmerge = new FastqTuple2String();
                FastqRDD = namedLinesRDD.map(RDDmerge);

                RDDTaggedUnitFilter RDDTagFilter = new RDDTaggedUnitFilter();
                FastqRDD = FastqRDD.filter(RDDTagFilter);
            }
        }else {

            FastqRDD = sc.textFile(param.inputFqPath);

            if (param.filterFastq == true){
                FastqFilterWithQual RDDConcatQ = new FastqFilterWithQual();
                FastqRDD = FastqRDD.map(RDDConcatQ);

                RDDUnitFilter RDDFilter = new RDDUnitFilter();
                FastqRDD = FastqRDD.filter(RDDFilter);
            }

        }

        if (param.inputFqLinePath != null && param.lineToFasta == false) {      // line to fastq
            LineToFastq RDDLineToFastq = new LineToFastq();
            FastqRDD = FastqRDD.map(RDDLineToFastq);

            RDDUnitFilter FastqUnitFilter = new RDDUnitFilter();
            FastqRDD = FastqRDD.filter(FastqUnitFilter);
        }

        if (param.lineToFasta == true){
            LineFilterToFasta RDDLineToFasta = new LineFilterToFasta();
            FastqRDD = FastqRDD.map(RDDLineToFasta);

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

        param.toolParam.trim();
        String command = param.toolDepend + " " + param.tool + " " + param.toolParam;
        info.readMessage(command);
        info.screenDump();

        FastqRDD = FastqRDD.pipe(command);

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
