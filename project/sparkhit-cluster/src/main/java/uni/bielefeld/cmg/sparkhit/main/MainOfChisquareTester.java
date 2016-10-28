package uni.bielefeld.cmg.sparkhit.main;


import org.apache.commons.cli.ParseException;
import uni.bielefeld.cmg.sparkhit.pipeline.Pipelines;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;
import uni.bielefeld.cmg.sparkhit.util.ParameterForChisquareTester;
import uni.bielefeld.cmg.sparkhit.util.ParameterForCluster;

import java.io.IOException;

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


public class MainOfChisquareTester {
    public static void main(String[] args){
        InfoDumper info = new InfoDumper();
        info.readParagraphedMessages("SparkHit ChisquareTester initiating ... \ninterpreting parameters.");
        info.screenDump();

        ParameterForChisquareTester parameterC = null;
        try {
            parameterC = new ParameterForChisquareTester(args);
        } catch (IOException e) {
            e.fillInStackTrace();
        } catch (ParseException e) {
            e.fillInStackTrace();
        }
        DefaultParam param = parameterC.importCommandLine();

        Pipelines pipes = new Pipelines();
        pipes.setParameter(param);
        pipes.sparkChisquareTester();

    }
}
