package uni.bielefeld.cmg.sparkhit.hadoop.decodec.sparkdoop;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

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


public class TokenizerMapperLineQ extends Mapper<Object, Text, Text, String>
{
    private Text word = new Text();
    private int lineMark = 0;
    private String line = "";

    public void map(Object key, Text value, Context context ){
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String s = itr.nextToken();
            if (lineMark == 2) {
                line = line + "\t" + s;
                lineMark = 3;
            }else if (lineMark == 3) {
                line = line + "\t" + s;
                lineMark = 4;
                word.set(line);
                try {
                    context.write(word, null);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else if (s.startsWith("@")){
                line = s;
                lineMark = 1;
            }else if (lineMark == 1) {
                line = line + "\t" + s;
                lineMark = 2;
            }
        }
    }
}
