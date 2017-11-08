package uni.bielefeld.cmg.sparkhit.main;


import org.apache.commons.cli.ParseException;
import uni.bielefeld.cmg.sparkhit.pipeline.Pipelines;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;
import uni.bielefeld.cmg.sparkhit.util.ParameterForParallelizer;
import uni.bielefeld.cmg.sparkhit.util.ParameterForReductioner;

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

/**
 * Main Java method specified in the Manifest file for running
 * Sparkhit-reductioner. It can also be specified at the input options
 * of Spark cluster mode during submission.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class MainOfReductioner {
    /**
     * This is the main Java method that starts the program and receives input
     * arguments.
     *
     * @param args a space separated command line arguments.
     */
    public static void main(String[] args){
        InfoDumper info = new InfoDumper();
        info.readParagraphedMessages("SparkHit Reductioner (PCA) initiating ... \ninterpreting parameters.");
        info.screenDump();

        ParameterForReductioner parameterR = null;
        try {
            parameterR = new ParameterForReductioner(args);
        } catch (IOException e) {
            e.fillInStackTrace();
        } catch (ParseException e) {
            e.fillInStackTrace();
        }
        DefaultParam param = parameterR.importCommandLine();

        Pipelines pipes = new Pipelines();
        pipes.setParameter(param);
        pipes.sparkReductioner();

    }
}
