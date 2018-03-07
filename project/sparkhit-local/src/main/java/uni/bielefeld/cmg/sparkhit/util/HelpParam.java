package uni.bielefeld.cmg.sparkhit.util;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Map;

import static java.lang.System.err;
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
public class HelpParam {
    private final Options parameter;
    private final Map<String, Integer> parameterMap;

    /**
     *
     * @param parameter
     * @param parameterMap
     */
    public HelpParam(Options parameter, Map<String, Integer> parameterMap){
        /**
         * utilizes HelpFormatter to dump command line information for using the pipeline
         */
        this.parameter = parameter;
        this.parameterMap = parameterMap;
    }

    /**
     * print out help info with parameters
     */
    public void printHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                            parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "java -jar SparkHit.jar");
        err.println("Name:");
        err.println("\tSparkHit standalone");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\tBuild reference : ");
        err.println(executable + " [parameters] -build reference.fa");
        err.println("\trun fragment recruitment : ");
        err.println(executable + " [parameters] -fastq query.fq -reference reference.fa -outfile output_file.txt");
        err.println();
    }
}
