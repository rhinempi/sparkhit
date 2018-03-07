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

import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns an object for parsing the input options for Sparkhit-parallelizer.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ParameterForParallelizer {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    /**
     * A constructor that construct an object of {@link Parameter} class.
     *
     * @param arguments an array of strings containing commandline options
     * @throws IOException
     * @throws ParseException
     */
    public ParameterForParallelizer(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    /* parameter IDs */
    private static final String
            INPUT_NODES = "nodes",
            INPUT_TOOL = "tool",
            TOOL_PARAM = "toolparam",
            TOOL_DEPEND = "tooldepend",
            OUTPUT_LINE = "outfile",
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

        parameterMap.put(INPUT_NODES, o++);
        parameterMap.put(INPUT_TOOL, o++);
        parameterMap.put(TOOL_PARAM, o++);
        parameterMap.put(TOOL_DEPEND, o++);
        parameterMap.put(OUTPUT_LINE, o++);
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

        parameter.addOption(OptionBuilder.withArgName("input nodes number")
                .hasArg().withDescription("Input nodes number for parallel action")
                .create(INPUT_NODES));

        parameter.addOption(OptionBuilder.withArgName("tool dependencies")
                .hasArg().withDescription("Use \"\" quotation to quote Dependencies for your tool. Or instead, put it in tool path in commandline logic. Default is NONE")
                .create(TOOL_DEPEND));

        parameter.addOption(OptionBuilder.withArgName("external tool path")
                .hasArg().withDescription("Path to an external tool you want to use, a script or a tool")
                .create(INPUT_TOOL));

        parameter.addOption(OptionBuilder.withArgName("external tool param")
                .hasArg().withDescription("Use \"\" quotation to quote Parameter for your tool, please exclude input and output for your tool as it should be STDIN and STDOUT for Spark Pipe. Please include reference and place it in the right position in your command line")
                .create(TOOL_PARAM));

        parameter.addOption(OptionBuilder.withArgName("output file")
                .hasArg().withDescription("Output sequencing data directory")
                .create(OUTPUT_LINE));

        parameter.addOption(OptionBuilder.withArgName("re-partition num")
                .hasArg().withDescription("even the load of each task, 1 partition for a task or 4 partitions for a task is recommended. Default, not re-partition")
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
                help.printParallelizerHelp();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)){
                help.printParallelizerHelp();
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

            if ((value = cl.getOptionValue(INPUT_NODES)) != null) {
                param.inputNodes = Integer.decode(value);
            }else {
                help.printParallelizerHelp();
                System.exit(0);
//                throw new IOException("Input file not specified.\nUse -help for list of options");
            }

			/* not applicable for HDFS and S3 */
            /* using TextFileBufferInput for such purpose */
//			File inputFastq = new File(param.inputFqPath).getAbsoluteFile();
//			if (!inputFastq.exists()){
//				err.println("Input query file not found.");
//				return;
//i			}

            if ((value = cl.getOptionValue(OUTPUT_LINE)) != null){
                param.outputPath = value;
            }else{
                help.printScriptPiperHelp();
                info.readMessage("Output file not set with -outfile options");
                info.screenDump();
                System.exit(0);
            }


            File outfile = new File(param.outputPath).getAbsoluteFile();
            if (outfile.exists()){
                info.readParagraphedMessages("Output file : \n\t" + param.outputPath + "\nalready exists, will be overwrite.");
                info.screenDump();
                Runtime.getRuntime().exec("rm -rf " + param.outputPath);
            }

            if ((value = cl.getOptionValue(INPUT_TOOL)) != null){
                param.tool = value.replaceAll("\"","");
                info.readParagraphedMessages("External tool locate at : \n\t" + param.tool + "\nplease ensure that all nodes have such tools in the same path.\nOr it is located in a shared file system.\nOr use a Docker container");
                info.screenDump();
            }else{
                help.printScriptPiperHelp();
                info.readMessage("External tool path have not been set with parameter -tool");
                info.screenDump();
                System.exit(0);
            }

            if ((value = cl.getOptionValue(TOOL_DEPEND)) != null){
                param.toolDepend= value.replaceAll("\"","");
                info.readParagraphedMessages("External tool Dependencies : \n\t" + param.toolDepend + "\n" );
                info.screenDump();
            }else{
                info.readMessage("External tool dependencies have not been set, will run with tool build-in Interpreter");
                info.screenDump();
            }

            if ((value = cl.getOptionValue(TOOL_PARAM)) != null){
                param.toolParam= value.replaceAll("\"","");
                info.readParagraphedMessages("External tool parameter : \n\t" + param.toolParam + "\nThe entire command looks like : \n\t" + param.toolDepend + " " + param.tool + " " + param.toolParam );
                info.screenDump();
                info.readParagraphedMessages("Final external command is : \n\t" + param.toolDepend + " " + param.tool + " " + param.toolParam);
                info.screenDump();
            }else{
                info.readMessage("External tool parameter have not been set, will run with its default parameter");
                info.screenDump();
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