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
public class ObjectFileInput implements InputFileManager {
    private String path;
    private File inputPath;
    public FileInputStream inputStream;
    public ObjectInputStream inputObjectStream;
    private InfoDumper info = new InfoDumper();

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

    public ObjectInputStream getInputObjectStream(){
        return inputObjectStream;
    }

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

    public void setInput(String inputFile){
        this.path = inputFile;
        checkFile(inputFile);
        setInputFile();
        setInputStream();
        setObjectInputStream();
    }

    public void bufferInputFile(String inputFile){
        this.path = inputFile;
    }

    public void bufferOutputFile(String outputFile){

    }
}
