package uni.bielefeld.cmg.sparkhit.main;


import org.apache.commons.cli.ParseException;
import uni.bielefeld.cmg.sparkhit.pipeline.Pipelines;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;
import uni.bielefeld.cmg.sparkhit.util.Parameter;

import java.io.IOException;

/**
 * Created by rhinempi on 23/01/16.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015
 *      Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
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
 *
 */


public class Main {

    public static void main(String[] args){
        InfoDumper info = new InfoDumper();
        info.readParagraphedMessages("SparkHit main initiating ... \ninterpreting parameters.");
        info.screenDump();

        Parameter parameter = null;
        try {
            parameter = new Parameter(args);
        } catch (IOException e) {
            e.fillInStackTrace();
        } catch (ParseException e) {
            e.fillInStackTrace();
        }
        DefaultParam param = parameter.importCommandLine();

        Pipelines pipes = new Pipelines();
        pipes.setParameter(param);
        pipes.spark();

    }

}
