package uni.bielefeld.cmg.sparkhit.main;


import org.apache.commons.cli.ParseException;
import uni.bielefeld.cmg.sparkhit.pipeline.Pipelines;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;
import uni.bielefeld.cmg.sparkhit.util.ParameterForParallelizer;
import uni.bielefeld.cmg.sparkhit.util.ParameterForReductioner;

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
