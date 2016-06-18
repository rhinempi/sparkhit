package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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


public class SparkReductionPipe implements Serializable{
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

        class PartitionIterator implements FlatMapFunction<Iterator<String>, Vector> {
            public Iterable<Vector> call(Iterator<String> input) {
                ArrayList<ArrayList<Integer>> snps = new ArrayList<ArrayList<Integer>>();
                ArrayList<Vector> snpsVector = new ArrayList<Vector>();

                for (int i = 0 ; i < 1000; i++){
                    snps.add(new ArrayList<Integer>());
                }

                while (input.hasNext()) {
                    String line = input.next();
                    if (line.startsWith("#")) {
                        line = input.next();
                    }

                    String[] array = line.split("\\t");

                    if (array.length <= 9){continue;}

                    int feature = 0;
                    for (int i = 9; i < 9 + 1000; i++) {
                        if (array[i].equals("0|0")) {
                            feature = 0;
                        } else if (array[i].equals("0|1") || array[i].equals("1|0")) {
                            feature = 1;
                        } else if (array[i].equals("1|1")) {
                            feature = 2;
                        }
                        snps.get(i-9).add(feature);
                    }

                }

                for (int i = 0; i < 1000; i++){
                    double[] vector = new double[snps.get(i).size()];
                    for (int j=0; j< snps.get(i).size(); j++){
                        vector[j] = (double)(snps.get(i).get(j));
                    }
                    snpsVector.add(Vectors.dense(vector));
                }

                return snpsVector;
            }
        }

        class PartitionIteratorBlock implements FlatMapFunction<Iterator<String>, Vector> {
            public Iterable<Vector> call(Iterator<String> input) {

                ArrayList<ArrayList<Integer>> snps = new ArrayList<ArrayList<Integer>>();
                ArrayList<Vector> snpsVector = new ArrayList<Vector>();
                int lineMark=0;
                int p2 = 0, pq = 0, q2 = 0;

                for (int i = 0 ; i < 1000; i++){
                    snps.add(new ArrayList<Integer>());
                }

                while (input.hasNext()) {
                    String line = input.next();
                    if (line.startsWith("#")) {
                        line = input.next();
                    }

                    String[] array = line.split("\\t");

                    lineMark++;

                    if (array.length <= 9) {
                        continue;
                    }

                    if (lineMark % param.window == 0){p2 = 0; pq = 0; q2 = 0;}

                    for (int i = 9; i < 9 + 1000; i++) {
                        if (array[i].equals("0|0")) {
                            p2++;
                        } else if (array[i].equals("0|1") || array[i].equals("1|0")) {
                            pq++;
                        } else if (array[i].equals("1|1")) {
                            q2++;
                        }

                        if (lineMark % param.window == 0) {
                            snps.get(i - 9).add(p2);
                            snps.get(i - 9).add(pq);
                            snps.get(i - 9).add(q2);
                        }

                    }

                }

                /* add last block */
                for (int i = 9; i < 9 + 1000; i++) {
                        snps.get(i - 9).add(p2);
                        snps.get(i - 9).add(pq);
                        snps.get(i - 9).add(q2);
                }

                for (int i = 0; i < 1000; i++){
                    double[] vector = new double[snps.get(i).size()];
                    for (int j=0; j< snps.get(i).size(); j++){
                        vector[j] = (double)(snps.get(i).get(j));
                    }
                    snpsVector.add(Vectors.dense(vector));
                }

                return snpsVector;
            }
        }

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

        ListRDD.cache();

        RowMatrix mat = new RowMatrix(ListRDD.rdd());

        Matrix pc = mat.computePrincipalComponents(3);

        RowMatrix projected = mat.multiply(pc);

        Vector[] collectPartitions = (Vector[]) projected.rows().collect();

        TextFileBufferOutput output = new TextFileBufferOutput();
        output.setOutput(param.outputPath, true);
        BufferedWriter outputBufferWriter = output.getOutputBufferWriter();
        for (Vector vector: collectPartitions){
            try {
                outputBufferWriter.write(vector.toString()+"\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public void setParam(DefaultParam param){
        this.param = param;
    }
}
