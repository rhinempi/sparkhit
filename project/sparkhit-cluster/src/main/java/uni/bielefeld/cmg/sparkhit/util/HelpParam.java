package uni.bielefeld.cmg.sparkhit.util;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Map;

import static java.lang.System.err;

/**
 * Created by Liren Huang on 13/01/16.
 *
 *      SparkHit
 *
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

/**
 * Returns an object for dumping help information to the screen.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class HelpParam {
    private final Options parameter;
    private final Map<String, Integer> parameterMap;

    /**
     * A constructor that construct an object of {@link HelpParam} class.
     *
     * @param parameter {@link Options} the commandline options.
     * @param parameterMap a {@link Map} that stores the parameter.
     */
    public HelpParam(Options parameter, Map<String, Integer> parameterMap){
        /**
         * utilizes HelpFormatter to dump command line information for using the pipeline
         */
        this.parameter = parameter;
        this.parameterMap = parameterMap;
    }

    /**
     * This method prints out help info for Sparkhit-recruiter
     */
    public void printHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                            parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.sparkhit.main.Main sparkhit.jar");
        final String executable2 = System.getProperty("executable2", "sparkhit mapper [spark parameter]");
        err.println("Name:");
        err.println("\tSparkHit recruiter");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\trun fragment recruitment : ");
        err.println(executable + " [parameters] -fastq query.fq -reference reference.fa -outfile output_file");
        err.println(executable + " [parameters] -line query.txt -reference reference.fa -outfile output_file");
        err.println(executable2 + " [parameters] -fastq query.fq -reference reference.fa -outfile output_file\"");
        err.println();
    }

    /**
     * This method prints out help info for Sparkhit-mapper
     */
    public void printMapperHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.sparkhit.main.MainOfMapper sparkhit.jar");
        final String executable2 = System.getProperty("executable2", "sparkhit mapper [spark parameter]");
        err.println("Name:");
        err.println("\tSparkHit Mapper");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\trun short read mapping : ");
        err.println(executable + " [parameters] -fastq query.fq -reference reference.fa -outfile output_file");
        err.println(executable + " [parameters] -line query.txt -reference reference.fa -outfile output_file");
        err.println(executable2 + " [parameters] -fastq query.fq -reference reference.fa -outfile output_file\"");
        err.println();
    }

    /**
     * This method prints out help info for Sparkhit-converter
     */
    public void printConverterHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.sparkhit.main.MainOfConverter Sparkhit.jar");
        final String executable2 = System.getProperty("executable2", "sparkhit converter [spark parameter]");
        err.println("Name:");
        err.println("\tSparkHit Converter");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\tConvert different file format :");
        err.println(executable + " [parameters] -fastq query.fq.tar.bz2 -outfile ./outdir");
        err.println(executable2 + " [parameters] -fastq query.fq.tar.bz2 -outfile ./outdir");
        err.println();
    }

    /**
     * This method prints out help info for Sparkhit-reporter
     */
    public void printReporterHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.sparkhit.main.MainOfReporter Sparkhit.jar");
        final String executable2 = System.getProperty("executable2", "sparkhit reporter [spark parameter]");
        err.println("Name:");
        err.println("\tSparkHit Reporter");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\tReport mapping summary");
        err.println(executable + " [parameters] -input ./sparkhit.out -outfile ./sparkhit.report");
        err.println(executable2 + " [parameters] -input ./sparkhit.out -outfile ./sparkhit.report");
        err.println();
    }

    /**
     * This method prints out help info for Sparkhit-piper
     */
    public void printScriptPiperHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.sparkhit.main.MainOfPiper Sparkhit.jar");
        final String executable2 = System.getProperty("executable2", "sparkhit piper [spark parameter]");
        err.println("Name:");
        err.println("\tSparkHit ScriptPiper (bwa, bowtie2 or other aligner)");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\tParallelize your own tool like bwa mem:");
        err.println(executable + " [parameters] -fastq query.fq.tar.bz2 -outfile ./outbams_dir -tool \"/mypath/bwa mem\" -toolparam \"/mypath/reference.fa -t 32\"");
        err.println(executable2 + " [parameters] -fastq query.fq.tar.bz2 -outfile ./outbams_dir -tool \"/mypath/bwa mem\" -toolparam \"/mypath/reference.fa -t 32\"");
        err.println();
    }

    /**
     * This method prints out help info for Sparkhit-parallelizer
     */
    public void printParallelizerHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.sparkhit.main.MainOfParallelizer Sparkhit.jar");
        final String executable2 = System.getProperty("executable2", "sparkhit parallelizer [spark parameter]");
        err.println("Name:");
        err.println("\tSparkHit Parallelizer");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\tParallelize an operation to each worker nodes:");
        err.println(executable + " [parameters] -nodes 10 -tool \"kill -u ec2-user\"");
        err.println(executable2 + " [parameters] -nodes 10 -tool \"kill -u ec2-user\"");
        err.println();
    }

    /**
     * This method prints out help info for Sparkhit-statisticer
     */
    public void printStatisticerHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.sparkhit.main.MainOfStatisticer Sparkhit.jar");
        final String executable2 = System.getProperty("executable2", "sparkhit [command] [spark parameter]");
        err.println("Name:");
        err.println("\tSparkHit Machine Learning library");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\tMachine learning library for vcf or tabular file:");
        err.println(executable + " [parameters] -vcf genotype.vcf -outfile ./result -column 2-10 -cache");
        err.println(executable2 + " [parameters] -vcf genotype.vcf -outfile ./result -column 2-10 -cache");
        err.println();
    }

    /**
     * This method prints out help info for Sparkhit-decompresser
     */
    public void printDecompresserHelp(){
        HelpFormatter formatter =new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>(){
            public int compare(Option o1, Option o2){
                return Integer.compare(parameterMap.get(o1.getOpt()),
                        parameterMap.get(o2.getOpt()));
            }
        });

        final String executable = System.getProperty("executable", "spark-submit [spark parameter] --class uni.bielefeld.cmg.sparkhit.main.MainOfDecompresser Sparkhit.jar");
        final String executable2 = System.getProperty("executable2", "sparkhit decompresser [spark parameter]");
        err.println("Name:");
        err.println("\tSparkHit Decompresser");
        err.println();
        err.println("Options:");
        formatter.printOptions(new PrintWriter(err, true), 85, parameter, 2, 3); /* print formatted parameters */
        err.println();
        err.println("Usage:");
        err.println("\tDecomress zipball and tarball using spark codec:");
        err.println(executable + " sparkhit.jar [parameters] -fastq input.fq.bz2 -outfile ./decompressed");
        err.println(executable2 + "[parameters] -fastq input.fq.bz2 -outfile ./decompressed");
        err.println();
    }

}
