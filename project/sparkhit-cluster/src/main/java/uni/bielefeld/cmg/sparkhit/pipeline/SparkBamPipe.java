package uni.bielefeld.cmg.sparkhit.pipeline;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.JavaDeserializationStream;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferInput;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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


public class SparkBamPipe implements Serializable{
    private DefaultParam param;
    private InfoDumper info = new InfoDumper();
    private BufferedReader bamListReader;
    private List<String> bamFile = new ArrayList<String>();
    private int slides;

    private SparkConf setSparkConfiguration(){
        SparkConf conf = new SparkConf().setAppName("SparkHit");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"); // default spark context setting
        conf.set("spark.kryo.registrator", "uni.bielefeld.cmg.sparkhit.serializer.SparkKryoRegistrator");

        return conf;
    }

    private void LoadBamList(String inputList){
        TextFileBufferInput bamListInput = new TextFileBufferInput();
        bamListInput.setInput(inputList);
        bamListReader = bamListInput.getBufferReader();
    }

    private void createRDDElement(){
        String bam;
        try {
            while ((bam = bamListReader.readLine())!=null){
                bamFile.add(bam);
            }
            slides = bamFile.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void spark() {
        LoadBamList(param.inputList);
        createRDDElement();

        SparkConf conf = setSparkConfiguration();
        info.readMessage("Initiating Spark context ...");
        info.screenDump();
        info.readMessage("Start Spark framework");
        info.screenDump();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> bamRDD = sc.parallelize(bamFile,slides);

        String command = param.toolDepend + " " + param.tool + " \"" + param.toolParam + "\"";
        info.readMessage(command);
        info.screenDump();

        JavaRDD<String> vcfRDD = bamRDD.pipe(command);

        vcfRDD.saveAsTextFile(param.outputPath);


    }

    public void setParam(DefaultParam param){
        this.param = param;
    }
}
