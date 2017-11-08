package uni.bielefeld.cmg.sparkhit.io;

import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.*;

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
 * Returns an object for buffering input fasta files. This class is
 * used for loading reference genomes located in local file system.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ReadFasta implements InputFileManager {
    private InfoDumper info = new InfoDumper();

    public BufferedReader fasta;

    /**
     * A constructor that construct an object of {@link ReadFasta} class.
     * No constructor option needed.
     */
    public ReadFasta(){
        /**
         *
         */
    }

    /**
     * This method sets up an input file stream for an input fasta file.
     *
     * @param inputFastaString full path of an input fasta file.
     */
    public void createInputFastaStream(String inputFastaString){
        try{
            FileInputStream inputFastaStream = new FileInputStream(inputFastaString);
            InputStreamReader inputFasta = new InputStreamReader(inputFastaStream);
            fasta = new BufferedReader(inputFasta);
        }catch (IOException e) {
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    /**
     * This method checks the path of an input fasta file. It classifies the
     * location (via URL) of an input file.
     *
     * @param cFile the full path of an input fasta file.
     */
    public void checkFile(String cFile){
        File cFilePath = new File(cFile);

        if (cFile.startsWith("s3")){
            info.readMessage("Input fasta file is located in S3 bucket : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fasta file using Map-Reduce FASTA reader");
            info.screenDump();
        }

        else if (cFile.startsWith("hdfs")) {
            info.readMessage("Input fasta file is lacated in HDFS : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fasta file using hadoop file reader");
            info.screenDump();
        }

        else{
            info.readMessage("Input fasta file is a local file : ");
            info.screenDump();
            info.readMessage("\t" + cFilePath.getAbsolutePath());
            info.screenDump();

            if (!cFilePath.exists()){
                info.readMessage("However, it is not there, please check it again");
                info.screenDump();
                System.exit(0);
            }else {
                info.readMessage("Reading fasta file using BufferedReader");
                info.screenDump();
            }
        }
    }

    /**
     * This method sets the BufferedReader.
     *
     * @return Java {@link BufferedReader}.
     */
    public BufferedReader getFastaBufferedReader(){
        return this.fasta;
    }

    /**
     * This method sets up an input fasta file stream based on an input file path.
     *
     * @param inputFile the full path of an input file.
     */
    public void bufferInputFile(String inputFile) {
        checkFile(inputFile);
        createInputFastaStream(inputFile);
    }

    /**
     * This method sets the full path of an output file.
     *
     * @param outputFile the full path of an output file.
     */
    public void bufferOutputFile(String outputFile){
        /**
         *
         */
    }
}
