package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;
import java.io.Serializable;

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
 * Returns an object for running the Sparkhit Chi-square test pipeline.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class SparkChiSquarePipe implements Serializable{
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

        class VariantToPValue implements Function<String, String> {
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of a VCF file.
             * @return the P value of the Chi-square test.
             */
            public String call(String s) {

                if (s.startsWith("#")) {
                    return null;
                }

                String[] array = s.split("\\t");

                if (array.length < param.column2End) {
                    return null;
                }

                int P =0, p= 0;
                for (int i = param.columnStart-1; i < param.columnEnd; i++) {
                    if (array[i].startsWith("0|0")) {
                        P+=2;
                    } else if (array[i].startsWith("0|1") || array[i].startsWith("1|0")) {
                        P++;p++;
                    } else if (array[i].startsWith("1|1")) {
                        p+=2;
                    }
                }

                int A = 0, a = 0;
                for (int i = param.column2Start-1; i < param.column2End; i++) {
                    if (array[i].startsWith("0|0")) {
                        A+=2;
                    } else if (array[i].startsWith("0|1") || array[i].startsWith("1|0")) {
                        A++;a++;
                    } else if (array[i].startsWith("1|1")) {
                        a+=2;
                    }
                }

                if ((P + p) ==0){
                    return array[0] + "\t" + array[1] + "\t" + array[2] + "\t" + 1;
                }

                if ((A + a) ==0){
                    return array[0] + "\t" + array[1] + "\t" + array[2] + "\t" + 1;
                }

                if (P+A==0){
                    return array[0] + "\t" + array[1] + "\t" + array[2] + "\t" + 1;
                }

                if (p+a==0){
                    return array[0] + "\t" + array[1] + "\t" + array[2] + "\t" + 1;
                }

                Matrix dm = Matrices.dense(2,2,new double[]{P, A, p, a});
                ChiSqTestResult independenceTestResult = Statistics.chiSqTest(dm);
                double pValue = independenceTestResult.pValue();
                return array[0] + "\t" + array[1] + "\t" + array[2] + "\t" + pValue;
            }
        }

        class TabVariantToPValue implements Function<String, String> {
            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of a tabular file.
             * @return the P value of the Chi-square test.
             */
            public String call(String s) {
                String[] array = s.split("\\t");
                String prefix="";
                double P = Double.parseDouble(array[param.column2Start-1]);
                double A = Double.parseDouble(array[param.column2Start]);
                double p = Double.parseDouble(array[param.column2Start+1]);
                double a = Double.parseDouble(array[param.column2Start+2]);
                for (int i=0;i<param.columnEnd-param.columnStart+1;i++){
                    prefix += array[i] + "\t";
                }

                Matrix dm = Matrices.dense(2,2,new double[]{P, A, p, a});
                ChiSqTestResult independenceTestResult = Statistics.chiSqTest(dm);
                double pValue = independenceTestResult.pValue();
                return prefix + pValue;
            }
        }

        class Filter implements Function<String, Boolean>, Serializable{

            /**
             * This function implements the Spark {@link Function}.
             *
             * @param s an input line of the reporter`s result.
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

        if (param.partitions != 0) {
            vcfRDD = vcfRDD.repartition(param.partitions);
        }

        vcfRDD.cache();

        JavaRDD<String> pValueRDD;

        if (param.inputTabPath==null) {
            VariantToPValue toPValue = new VariantToPValue();
            pValueRDD= vcfRDD.map(toPValue);
        }else {
            TabVariantToPValue toPValue = new TabVariantToPValue();
            pValueRDD = vcfRDD.map(toPValue);
        }

        Filter RDDFilter = new Filter();
        pValueRDD = pValueRDD.filter(RDDFilter);

        pValueRDD.saveAsTextFile(param.outputPath);
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
