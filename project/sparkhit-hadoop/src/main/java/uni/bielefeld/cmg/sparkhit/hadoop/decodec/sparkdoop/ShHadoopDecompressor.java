package uni.bielefeld.cmg.sparkhit.hadoop.decodec.sparkdoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import uni.bielefeld.cmg.sparkhit.hadoop.decodec.util.DefaultParam;

import java.io.IOException;


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


public class ShHadoopDecompressor implements ShDecompressor{
    public DefaultParam param;

    public int run(){
        Configuration conf = new Configuration();
        int exitstatus = 1;
        try {
            Job job = new Job(conf, "Sparkhit hadoop (SpaDoop) decompressor");
            Path inPath = new Path(param.inputFqPath);
            Path outPath = new Path(param.outputPath);
            FileSystem fs = FileSystem.get(conf);
            fs.delete(outPath, param.overwrite);

            job.setJarByClass(ShHadoopDecompressor.class);
            if (param.outputFormat == 0) {
                job.setMapperClass(TokenizerMapper.class);
            }else if (param.outputFormat == 1){
                job.setMapperClass(TokenizerMapperFasta.class);
            }else if (param.outputFormat == 2){
                job.setMapperClass(TokenizerMapperFastq.class);
            }else if (param.outputFormat == 3){
                job.setMapperClass(TokenizerMapperLineQ.class);
            }
            job.setMapOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            if (param.bz2) {
                job.setInputFormatClass(Bzip2TextInputFormat.class);
            }else if (param.gz) {
                job.setInputFormatClass(PigTextInputFormat.class);
            }else{
                job.setInputFormatClass(TextInputFormat.class);
            }
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setNumReduceTasks(0);
            FileInputFormat.addInputPath(job, inPath);
            if (param.splitsize > 0 && param.splitsize <=128000000) {
                Bzip2TextInputFormat.setMaxInputSplitSize(job, param.splitsize);
            }else if (param.splitsize > 128000000){
                Bzip2TextInputFormat.setMinInputSplitSize(job, param.splitsize);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            exitstatus = job.waitForCompletion(true) ? 0 : 1;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        } catch (InterruptedException e){

        }
        return exitstatus;
    }

    public void setParameter(DefaultParam param){
        this.param = param;
    }
}
