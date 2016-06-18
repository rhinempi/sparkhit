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
 * Created by Liren Huang on 02/04/16.
 * <p/>
 * SparkHit-HadoopDecompression
 * <p/>
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 * <p/>
 * SparkHit-HadoopDecompression is free software: you can redistribute it and/or modify it
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
