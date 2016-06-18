package uni.bielefeld.cmg.sparkhit.hadoop.decodec.pipeline;


import uni.bielefeld.cmg.sparkhit.hadoop.decodec.sparkdoop.ShHadoopDecompressor;
import uni.bielefeld.cmg.sparkhit.hadoop.decodec.util.DefaultParam;

import java.io.BufferedReader;
import java.io.BufferedWriter;

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


public class Pipelines implements Pipeline {
    public DefaultParam param;
    public ShHadoopDecompressor spadoop;

    public int FastqDecompression(){
        setShHadoopDecompressor();
        spadoop.setParameter(param);
        return spadoop.run();
    }

    public void setShHadoopDecompressor(){
        this.spadoop = new ShHadoopDecompressor();
    }

    public void setParameter(DefaultParam param){
        this.param = param;
    }

    public void setInput(BufferedReader InputRead){

    }

    public void setOutput(BufferedWriter OutputWrite){

    }
}
