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
 * Returns an object for buffering input object files. This class is
 * used in local mode only. For cluster mode, Spark "textFile" function
 * is used to access input Fastq file.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class ObjectFileInput implements InputFileManager {
    private String path;
    private File inputPath;
    public FileInputStream inputStream;
    public ObjectInputStream inputObjectStream;
    private InfoDumper info = new InfoDumper();

    /**
     * This method checks the path of input file. It classifies the
     * location (via URL) of an input file.
     *
     * @param inputFile full path of an input file.
     */
    public void checkFile(String inputFile) {

        if (inputFile.startsWith("s3")) {
            info.readMessage("Input file is located in S3 bucket : ");
            info.screenDump();
            info.readMessage("\t" + inputFile);
            info.screenDump();
            info.readMessage("Reading file using Map-Reduce FASTA reader");
            info.screenDump();
        } else if (inputFile.startsWith("hdfs")) {
            info.readMessage("Input file is located in HDFS : ");
            info.screenDump();
            info.readMessage("\t" + inputFile);
            info.screenDump();
            info.readMessage("Reading file using hadoop file reader");
            info.screenDump();
        } else {
            info.readMessage("Input index file is a local file : ");
            info.screenDump();
            info.readMessage("\t" + inputFile);
            info.screenDump();

            File cFile = new File(path).getAbsoluteFile();
            if (!cFile.exists()) {
                info.readMessage("However, it is not there, please check it again !");
                info.screenDump();
                System.exit(0);
            } else {
                info.readMessage("Reading index file using FileInputStream");
                info.screenDump();
            }
        }
    }

    /**
     * This method sets ObjectInputStream.
     *
     *
     * @return Java {@link ObjectInputStream}.
     */
    public ObjectInputStream getInputObjectStream(){
        return inputObjectStream;
    }

    /**
     * This method sets FileInputStream.
     *
     * @return Java {@link FileInputStream}.
     */
    public FileInputStream getInputStream(){
        return inputStream;
    }

    private void setObjectInputStream(){
        try {
            this.inputObjectStream = new ObjectInputStream(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setInputStream(){
        try {
            this.inputStream = new FileInputStream(inputPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(0);
        }

    }

    private void setInputFile(){
        this.inputPath = new File(path);
    }

    /**
     * This method sets up an input file stream based on an input file path.
     *
     * @param inputFile full path of an input file.
     */
    public void setInput(String inputFile){
        this.path = inputFile;
        checkFile(inputFile);
        setInputFile();
        setInputStream();
        setObjectInputStream();
    }

    /**
     * This method sets the full path of an input file.
     *
     * @param inputFile the full path of an input file.
     */
    public void bufferInputFile(String inputFile){
        this.path = inputFile;
    }

    /**
     * This method is deprecated.
     *
     * @param outputFile the full path of an output file.
     */
    public void bufferOutputFile(String outputFile){

    }
}
