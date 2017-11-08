package uni.bielefeld.cmg.sparkhit.util;

/**
 * Created by Liren Huang on 13/01/16.
 *
 *      SparkHit
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */

import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns an object for parsing the input options for Sparkhit-regressioner.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ParameterForRegressioner {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    /**
     * A constructor that construct an object of {@link Parameter} class.
     *
     * @param arguments an array of strings containing commandline options
     * @throws IOException
     * @throws ParseException
     */
    public ParameterForRegressioner(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    /* parameter IDs */
    private static final String
            INPUT_TRAIN= "train",
            INPUT_VCF = "vcf",
            INPUT_TAB = "tab",
            OUTPUT_LINE = "outfile",
            MODELS = "model",
            WINDOW = "window",
            COLUMN = "column",
            CACHE = "cache",
            ITERATION= "iteration",
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

        parameterMap.put(INPUT_TRAIN, o++);
        parameterMap.put(INPUT_VCF, o++);
        parameterMap.put(INPUT_TAB, o++);
        parameterMap.put(OUTPUT_LINE, o++);
        parameterMap.put(MODELS, o++);
        parameterMap.put(WINDOW, o++);
        parameterMap.put(COLUMN, o++);
        parameterMap.put(CACHE, o++);
        parameterMap.put(ITERATION, o++);
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

        parameter.addOption(OptionBuilder.withArgName("training data")
                .hasArg().withDescription("Input vcf file containing training data")
                .create(INPUT_TRAIN));

        parameter.addOption(OptionBuilder.withArgName("input VCF file")
                .hasArg().withDescription("Input vcf file containing variation info")
                .create(INPUT_VCF));

        parameter.addOption(OptionBuilder.withArgName("input tabular file")
                .hasArg().withDescription("Input tabular file containing variation info")
                .create(INPUT_TAB));

        parameter.addOption(OptionBuilder.withArgName("output file")
                .hasArg().withDescription("Output cluster index file")
                .create(OUTPUT_LINE));

        parameter.addOption(OptionBuilder.withArgName("cluster model")
                .hasArg().withDescription("clustering model, 0 for hierarchical, 1 for centroid (k-mean), default is " + param.model)
                .create(MODELS));

        parameter.addOption(OptionBuilder.withArgName("SNP window size")
                .hasArg().withDescription("window size for a block of snps")
                .create(WINDOW));

        parameter.addOption(OptionBuilder.withArgName("Columns for Alleles")
                .hasArg().withDescription("columns where allele info is set, default is " + param.columns)
                .create(COLUMN));

        parameter.addOption(OptionBuilder.withArgName("Cache data")
                .hasArg(false).withDescription("weather to cache data in memory or not, default no")
                .create(CACHE));

        parameter.addOption(OptionBuilder.withArgName("Iteration number")
                .hasArg().withDescription("how many iterations for learning, default is " + param.iterationNum)
                .create(ITERATION));

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
                help.printStatisticerHelp();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)){
                help.printStatisticerHelp();
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

            if ((value = cl.getOptionValue(WINDOW)) != null){
                param.window = Integer.decode(value);
            }

            if ((value = cl.getOptionValue(COLUMN)) != null){
                param.columns = value;
                param.columnStart = Integer.decode(value.split("-")[0]);
                param.columnEnd = Integer.decode(value.split("-")[1]);
            }else{
                param.columnStart = Integer.decode(param.columns.split("-")[0]);
                param.columnEnd = Integer.decode(param.columns.split("-")[1]);
            }

            if (cl.hasOption(CACHE)){
                param.cache =true;
            }

            if ((value = cl.getOptionValue(MODELS)) != null) {
                param.model = Integer.decode(value);
            }



            if ((value = cl.getOptionValue(ITERATION)) != null) {
                param.iterationNum = Integer.decode(value);
            }

            if ((value = cl.getOptionValue(INPUT_VCF)) != null) {
                param.inputFqPath = value;
            }else if ((value = cl.getOptionValue(INPUT_TAB)) != null) {
                param.inputFqPath = value;
            }else {
                help.printStatisticerHelp();
                System.exit(0);
//                throw new IOException("Input file not specified.\nUse -help for list of options");
            }

            if ((value = cl.getOptionValue(INPUT_TRAIN)) !=null){
                param.inputTrainPath = value;
            }else{
                help.printStatisticerHelp();
                System.exit(0);
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
                help.printStatisticerHelp();
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