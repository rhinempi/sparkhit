package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferOutput;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
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
 * Returns an object for running the Sparkhit reduction pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkReductionPipe implements Serializable{
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

        class PartitionIterator implements FlatMapFunction<Iterator<String>, Vector> {
            /**
             * This function implements the Spark {@link FlatMapFunction}.
             *
             * @param input an iterator (a list) of a VCF file.
             * @return a vector of variants from the VCF file.
             */
            public Iterator<Vector> call(Iterator<String> input) {
                ArrayList<ArrayList<Double>> snps = new ArrayList<ArrayList<Double>>();
                ArrayList<Vector> snpsVector = new ArrayList<Vector>();

                for (int i = 0 ; i <= param.columnEnd - param.columnStart; i++){
                    snps.add(new ArrayList<Double>());
                }

                while (input.hasNext()) {
                    String line = input.next();
                    if (line.startsWith("#")) {
                        continue;
                    }

                    String[] array = line.split("\\t");

                    if (array.length < param.columnEnd){continue;}

                    double feature = 0;
                    for (int i = param.columnStart-1; i <param.columnEnd; i++) {
                        if (param.inputTabPath!=null){
                            snps.get(i - param.columnStart + 1).add(Double.parseDouble(array[i]));
                        }else {
                            if (array[i].startsWith("0|0")) {
                                feature = 0;
                            } else if (array[i].startsWith("0|1") || array[i].startsWith("1|0")) {
                                feature = 1;
                            } else if (array[i].startsWith("1|1")) {
                                feature = 2;
                            }
                            snps.get(i - param.columnStart + 1).add(feature);
                        }
                    }

                }

                for (int i = 0; i <= param.columnEnd - param.columnStart; i++){
                    double[] vector = new double[snps.get(i).size()];
                    for (int j=0; j< snps.get(i).size(); j++){
                        vector[j] = snps.get(i).get(j);
                    }
                    snpsVector.add(Vectors.dense(vector));
                }

                return snpsVector.iterator();
            }
        }

        class PartitionIteratorBlock implements FlatMapFunction<Iterator<String>, Vector> {

            /**
             * This function implements the Spark {@link FlatMapFunction}.
             *
             * @param input an iterator (a list) of a VCF file.
             * @return a vector of variants from the VCF file.
             */
            public Iterator<Vector> call(Iterator<String> input) {

                ArrayList<ArrayList<Integer>> snps = new ArrayList<ArrayList<Integer>>();
                ArrayList<Vector> snpsVector = new ArrayList<Vector>();
                int lineMark=-1;
                int p2 = 0, pq = 0, q2 = 0;

                for (int i = 0 ; i <= param.columnEnd-param.columnStart; i++){
                    snps.add(new ArrayList<Integer>());
                }

                while (input.hasNext()) {
                    String line = input.next();
                    if (line.startsWith("#")) {
                        continue;
                    }

                    String[] array = line.split("\\t");

                    if (array.length < param.columnEnd) {
                        continue;
                    }

                    lineMark++;

                    if (lineMark % param.window == 0){p2 = 0; pq = 0; q2 = 0;}
                    int blockNum = 3 * (lineMark / param.window);

                    for (int i = param.columnStart-1; i < param.columnEnd; i++) {
                        if (lineMark % param.window == 0) {
                            snps.get(i - param.columnStart+1).add(p2);
                            snps.get(i - param.columnStart+1).add(pq);
                            snps.get(i - param.columnStart+1).add(q2);
                        }

                        if (array[i].startsWith("0|0")) {
                            snps.get(i - param.columnStart + 1).set(blockNum ,snps.get(i - param.columnStart + 1).get(blockNum) + 1);
                        } else if (array[i].startsWith("0|1") || array[i].startsWith("1|0")) {
                            snps.get(i - param.columnStart + 1).set(blockNum+1 ,snps.get(i - param.columnStart + 1).get(blockNum+1) + 1);
                        } else if (array[i].startsWith("1|1")) {
                            snps.get(i - param.columnStart + 1).set(blockNum+2 ,snps.get(i - param.columnStart + 1).get(blockNum+2) + 1);
                        }

                    }

                }

                /* add last block */
                /*
                for (int i = param.columnStart; i <= param.columnEnd; i++) {
                        snps.get(i - param.columnStart).add(p2);
                        snps.get(i - param.columnStart).add(pq);
                        snps.get(i - param.columnStart).add(q2);
                }
*/
                for (int i = 0; i <= param.columnEnd - param.columnStart; i++){
                    double[] vector = new double[snps.get(i).size()];
                    for (int j=0; j< snps.get(i).size(); j++){
                        vector[j] = ((double)snps.get(i).get(j))/param.window;
                    }
                    snpsVector.add(Vectors.dense(vector));
                }

                return snpsVector.iterator();
            }
        }

        class VariantToVector implements Function<String, Vector> {
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s a line of the input VCF file.
             * @return a vector of variants from the VCF file.
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
                    if (param.inputTabPath!=null){
                        vector[i-param.columnStart + 1] = Double.parseDouble(array[i]);
                    }else {
                        if (array[i].startsWith("0|0")) {
                            vector[i - param.columnStart + 1] = 0;
                        } else if (array[i].startsWith("0|1") || array[i].startsWith("1|0")) {
                            vector[i - param.columnStart + 1] = 1;
                        } else if (array[i].startsWith("1|1")) {
                            vector[i - param.columnStart + 1] = 2;
                        }
                    }
                }
                return Vectors.dense(vector);
            }
        }

        class Filter implements Function<Vector, Boolean>, Serializable {
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the reduction result.
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

        if (!param.horizontal) {

            if (param.partitions != 0) {
                vcfRDD = vcfRDD.repartition(param.partitions);
            }

            VariantToVector toVector = new VariantToVector();
            JavaRDD<Vector> vectorRDD = vcfRDD.map(toVector);

            Filter RDDFilter = new Filter();
            vectorRDD = vectorRDD.filter(RDDFilter);

            if (param.cache) {
                vectorRDD.cache();
            }

            RowMatrix mat = new RowMatrix(vectorRDD.rdd());

            Matrix pc = mat.computePrincipalComponents(param.componentNum);

            RowMatrix projected = mat.multiply(pc);

            Vector[] collectPartitions = (Vector[]) projected.rows().collect();


            TextFileBufferOutput output = new TextFileBufferOutput();
            output.setOutput(param.outputPath, true);
            BufferedWriter outputBufferWriter = output.getOutputBufferWriter();
            for (Vector vector : collectPartitions) {
                try {
                    outputBufferWriter.write(vector.toString() + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                outputBufferWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if (param.horizontal){
            JavaRDD<Vector> ListRDD;
            if (param.window == 0) {
                PartitionIterator VCFToVectorRDD = new PartitionIterator();
                ListRDD = vcfRDD.mapPartitions(VCFToVectorRDD);
            }else{
                PartitionIteratorBlock VCFToVectorBlockRDD = new PartitionIteratorBlock();
                ListRDD = vcfRDD.mapPartitions(VCFToVectorBlockRDD);
            }

            if (param.partitions != 0) {
                ListRDD = ListRDD.repartition(param.partitions);
            }

            if (param.cache) {
                ListRDD.cache();
            }

            RowMatrix mat = new RowMatrix(ListRDD.rdd());

            Matrix pc = mat.computePrincipalComponents(param.componentNum);

            RowMatrix projected = mat.multiply(pc);

            Vector[] collectPartitions = (Vector[]) projected.rows().collect();


            TextFileBufferOutput output = new TextFileBufferOutput();
            output.setOutput(param.outputPath, true);
            BufferedWriter outputBufferWriter = output.getOutputBufferWriter();
            for (Vector vector : collectPartitions) {
                try {
                    outputBufferWriter.write(vector.toString() + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                outputBufferWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
