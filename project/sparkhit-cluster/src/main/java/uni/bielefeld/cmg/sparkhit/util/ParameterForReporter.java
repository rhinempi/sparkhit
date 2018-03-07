package uni.bielefeld.cmg.sparkhit.util;

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

import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns an object for parsing the input options for Sparkhit-reporter.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ParameterForReporter {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    /**
     * A constructor that construct an object of {@link Parameter} class.
     *
     * @param arguments an array of strings containing commandline options
     * @throws IOException
     * @throws ParseException
     */
    public ParameterForReporter(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    /* parameter IDs */
    private static final String
            INPUT_HIT = "input",
            INPUT_KEY = "word",
            INPUT_VALUE = "count",
            OUTPUT_REPORT = "outfile",
            PARTITIONS = "partition",
            VERSION = "version",
            HELP2 = "h",
            HELP = "help";

    private static final Map<String, Integer> parameterMap = new HashMap<String, Integer>();

    /**
     * This method places all input parameters into a hashMap.
     */
    public void putParameterID(){
        int o =0;

        parameterMap.put(INPUT_HIT, o++);
        parameterMap.put(INPUT_KEY, o++);
        parameterMap.put(INPUT_VALUE, o++);
        parameterMap.put(OUTPUT_REPORT, o++);
        parameterMap.put(PARTITIONS, o++);
        parameterMap.put(VERSION, o++);
        parameterMap.put(HELP, o++);
        parameterMap.put(HELP2, o++);
    }

    /**
     * This method adds descriptions to each parameter.
     */
    public void addParameterInfo(){


		/* use Object parameter of Options class to store parameter information */

        parameter.addOption(OptionBuilder.withArgName("input sparkhit result file")
                .hasArg().withDescription("Input spark hit result file in tabular format. Accept wild card, s3n schema, hdfs schema")
                .create(INPUT_HIT));

        parameter.addOption(OptionBuilder.withArgName("columns for identifier")
                .hasArg().withDescription("a list of column number used to represent a category you want to summarize. eg, 1,3,8 means counting column 1 (chr), column 3 (strain), column 8 (identity)")
                .create(INPUT_KEY));

        parameter.addOption(OptionBuilder.withArgName("column for count number")
                .hasArg().withDescription("the number of column value which will be used to aggregate for the report. set to 0 the value will be 1 for every identifier")
                .create(INPUT_VALUE));

        parameter.addOption(OptionBuilder.withArgName("output report file")
                .hasArg().withDescription("Output report file in text format")
                .create(OUTPUT_REPORT));

        parameter.addOption(OptionBuilder.withArgName("re-partition number")
                .hasArg().withDescription("re generate number of partitions for .gz data, as .gz data only have one partition (spark parallelization)")
                .create(PARTITIONS));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("show version information")
                .create(VERSION));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("print and show this information")
                .create(HELP));

        parameter.addOption(OptionBuilder
                .hasArg(false).withDescription("")
                .create(HELP2));

    }

    /* main method */

    /**
     * This method parses input commandline arguments and sets correspond
     * parameters.
     *
     * @return {@link DefaultParam}.
     */
    public DefaultParam importCommandLine() {

        /* Assigning Parameter ID to an ascending number */
        putParameterID();

        /* Assigning parameter descriptions to each parameter ID */
        addParameterInfo();

        /* need a Object parser of PosixParser class for the function parse of CommandLine class */
        PosixParser parser = new PosixParser();

        /* print out help information */
        HelpParam help = new HelpParam(parameter, parameterMap);

        /* check each parameter for assignment */
        try {
            long input_limit = -1;
            int threads = Runtime.getRuntime().availableProcessors();

			/* Set Object cl of CommandLine class for Parameter storage */
            CommandLine cl = parser.parse(parameter, arguments, true);
            if (cl.hasOption(HELP)) {
                help.printReporterHelp();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)){
                help.printReporterHelp();
                System.exit(0);
            }

            if (cl.hasOption(VERSION)){
               System.exit(0);
            }

			/* Checking all parameters */

            String value;

            if ((value = cl.getOptionValue(PARTITIONS)) != null){
                param.partitions = Integer.decode(value);
            }

            if ((value = cl.getOptionValue(INPUT_KEY)) != null){
                param.word = value;
            }

            if ((value = cl.getOptionValue(INPUT_VALUE)) != null){
                param.count = Integer.decode(value);
            }

            if ((value = cl.getOptionValue(INPUT_HIT)) != null) {
                param.inputResultPath = value;
            }else {
                help.printReporterHelp();
                System.exit(0);
               // throw new IOException("Input file not specified.\nUse -help for list of options");
            }

			/* not applicable for HDFS and S3 */
            /* using TextFileBufferInput for such purpose */
//			File inputFastq = new File(param.inputFqPath).getAbsoluteFile();
//			if (!inputFastq.exists()){
//				err.println("Input query file not found.");
//				return;
//i			}

            if ((value = cl.getOptionValue(OUTPUT_REPORT)) != null){
                param.outputPath = value;
            }else{
                info.readMessage("Output file not set of -outfile options");
                info.screenDump();
            }


            File outfile = new File(param.outputPath).getAbsoluteFile();
            if (outfile.exists()){
                info.readParagraphedMessages("Output file : \n\t" + param.outputPath + "\nalready exists, will be overwrite.");
                info.screenDump();
                Runtime.getRuntime().exec("rm -rf " + param.outputPath);
            }

           // param.bestNas = (param.alignLength * param.readIdentity) / 100;
           // param.bestKmers = param.alignLength - (param.alignLength - param.bestNas) * 4 - 3;

        } catch (IOException e) { // Don`t catch this, NaNaNaNa, U can`t touch this.
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        } catch (RuntimeException e){
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        } catch (ParseException e){
            info.readMessage("Parameter settings incorrect.");
            info.screenDump();
            e.printStackTrace();
            System.exit(0);
        }

        return param;
    }
}