package uni.bielefeld.cmg.sparkhit.hadoop.decodec.util;

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

import javax.xml.bind.annotation.XmlElementDecl;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Parameter {
    private String[] arguments;
    private InfoDumper info = new InfoDumper();

    public Parameter(String[] arguments) throws IOException, ParseException {
        this.arguments = arguments;
    }

    private static final Options parameter = new Options();

    DefaultParam param = new DefaultParam();

    /* parameter IDs */
    private static final String
            INPUT_BZ2 = "bz2",
            INPUT_GZ = "gz",
            INPUT_SPLIT = "insplit",
            OUTPUT_FILE = "outfile",
            OUTPUT_FORMAT = "outfm",
            OUTPUT_OVERWRITE = "overwrite",
            VERSION = "version",
            HELP2 = "h",
            HELP = "help";

    private static final Map<String, Integer> parameterMap = new HashMap<String, Integer>();


    public void putParameterID(){
        int o =0;

        parameterMap.put(INPUT_BZ2, o++);
        parameterMap.put(INPUT_GZ, o++);
        parameterMap.put(INPUT_SPLIT, o++);
        parameterMap.put(OUTPUT_FILE, o++);
        parameterMap.put(OUTPUT_FORMAT, o++);
        parameterMap.put(OUTPUT_OVERWRITE, o++);
        parameterMap.put(VERSION, o++);
        parameterMap.put(HELP, o++);
        parameterMap.put(HELP2, o++);
    }

    public void addParameterInfo(){


		/* use Object parameter of Options class to store parameter information */
        parameter.addOption(OptionBuilder.withArgName("input bz2 file")
                .hasArg().withDescription("Input Next Generation Sequencing (NGS) data, fastq .bz2 file")
                .create(INPUT_BZ2));

        parameter.addOption(OptionBuilder.withArgName("input gz file")
                .hasArg().withDescription("Input Next Generation Sequencing (NGS) data, fastq .gz file")
                .create(INPUT_GZ));

        parameter.addOption(OptionBuilder.withArgName("input file split")
                .hasArg().withDescription("Split input file for parallelization, use Hadoop FileInputFormat InputSplit")
                .create(INPUT_SPLIT));

        parameter.addOption(OptionBuilder.withArgName("output file")
                .hasArg().withDescription("Output line based file in text format")
                .create(OUTPUT_FILE));

        parameter.addOption(OptionBuilder.withArgName("output file format")
                .hasArg().withDescription("0 output fastq units in lines, 1 output fastq units to fasta, 2 still ouput fastq, 3 output fastq units in one line, default in 0")
                .create(OUTPUT_FORMAT));

        parameter.addOption(OptionBuilder.withArgName("overwrite output")
                .hasArg(false).withDescription("set to overwrite output file if exists")
                .create(OUTPUT_OVERWRITE));

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
                help.printHelp();
                System.exit(0);
            }

            if (cl.hasOption(HELP2)){
                help.printHelp();
                System.exit(0);
            }

            if (cl.hasOption(VERSION)){
               System.exit(0);
            }

			/* Checking all parameters */

            String value;

            if ((value = cl.getOptionValue(INPUT_BZ2)) != null){
                param.inputFqPath = value;
                param.bz2 = true;
            }

			/* not applicable for HDFS and S3 */
            /* using TextFileBufferInput for such purpose */
//			File inputFastq = new File(param.inputFqPath).getAbsoluteFile();
//			if (!inputFastq.exists()){
//				err.println("Input query file not found.");
//				return;
//i			}
            if ((value = cl.getOptionValue(INPUT_GZ)) != null){
                param.inputFqPath = value;
                param.gz = true;
            }else if (cl.getOptionValue(INPUT_BZ2) == null){
                help.printHelp();
                System.exit(0);
                //throw new IOException("Input query file not specified.\nUse -help for list of options");
            }

            if ((value = cl.getOptionValue(INPUT_SPLIT)) != null){
                param.splitsize = Long.decode(value);
            }

            if ((value = cl.getOptionValue(OUTPUT_FILE)) != null){
                param.outputPath = value;
            }else{
                info.readMessage("Output file not set of -outfile options");
                info.screenDump();
            }

            if (cl.hasOption(OUTPUT_OVERWRITE)){
               param.overwrite = true;
            }

            if ((value = cl.getOptionValue(OUTPUT_FORMAT)) != null){
                param.outputFormat = Integer.decode(value);
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