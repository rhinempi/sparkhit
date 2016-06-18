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

public class ReadFastq implements InputFileManager {
    private InfoDumper info = new InfoDumper();
    private readInfo read = new readInfo();
    private String inputFastq;

    public BufferedReader fastq;
    public FastqUnitBuffer BufferedFastqUnit;

    public ReadFastq(){
        /**
         *  not used here
         */
    }

    /**
     *
     * @return
     */
    public FastqUnitBuffer getBufferedFastqUnit(){
        return this.BufferedFastqUnit;
    }

    public FastqUnitBuffer loadBufferedFastq(int units){
        try {
            String line;
            int lineMark = 0;
            int lineCount = 0;
            int unitsMark = 0;
            BufferedFastqUnit = new FastqUnitBuffer(fastq);
            while ((line = fastq.readLine()) != null) {
                lineCount++;
                if (lineMark == 0){
                    if (!line.startsWith("@")){
                        info.readMessage("Line \"" + lineCount + "\" does not started with \"@\" as a fastq file unit, skip to the next line.");
                        info.screenDump();
                        continue;
                    }else {
                        read.readId = line;
                        lineMark++;
                        continue;
                    }
                }

                if (lineMark == 1){
                    read.readSeq = line;
                    lineMark++;
                    continue;
                }

                if (lineMark == 2){
                    read.readPlus = line;
                    lineMark++;
                    continue;
                }

                if (lineMark == 3){
                    read.readQual = line;
                    BufferedFastqUnit.addReadUnit(read, unitsMark);
                    System.out.println("add one unit");
                    unitsMark++;
                    if (unitsMark >= units){
                        return BufferedFastqUnit;
                    }
                    lineMark = 0;
                }
            }
        }catch(IOException e){
            e.fillInStackTrace();
            System.exit(0);
        }
        return BufferedFastqUnit;
    }

    /**
     * Only when you want to creat bufferedReader with this class.
     * Usually I use TextFileBufferInput to create a synchronized
     * BufferedReader and parallelize getRead in threads.
     *
     * @param inputFastaString
     * @return
     */
    public void createInputFastqStream(String inputFastaString){
        try{
            FileInputStream inputFastaStream = new FileInputStream(inputFastaString);
            InputStreamReader inputFastq = new InputStreamReader(inputFastaStream);
            fastq = new BufferedReader(inputFastq);
        }catch (IOException e) {
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    /**
     *
     * @param cFile
     */
    public void checkFile(String cFile){

        if (cFile.startsWith("s3")){
            info.readMessage("Input fasta file is located in S3 bucket : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fasta file using Map-Reduce FASTA reader");
            info.screenDump();
        }

        else if (cFile.startsWith("hdfs")) {
            info.readMessage("Input fastq file is located in HDFS : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();
            info.readMessage("Reading fastq file using hadoop file reader");
            info.screenDump();
        }

        else{
            info.readMessage("Input fastq file is a local file : ");
            info.screenDump();
            info.readMessage("\t" + cFile);
            info.screenDump();

            File inputFile = new File(cFile).getAbsoluteFile();
            if (!inputFile.exists()){
                info.readMessage("However, it is not there, please check it again");
                info.screenDump();
                System.exit(0);
            } else if (cFile.endsWith("gz")) {
                info.readMessage("Reading fastq file using BufferedReader");
                info.screenDump();
                info.readMessage("Uncompress gz file using GzipCompressor");
                info.screenDump();
            } else if (cFile.endsWith("bz2")) {
                info.readMessage("Reading fastq file using BufferedReader");
                info.screenDump();
                info.readMessage("Uncompress bz2 file using BZip2Compressor");
                info.screenDump();
            }else {
                info.readMessage("Reading fastq file using BufferedReader");
                info.screenDump();
            }
        }
    }

    public void setInputBufferedReader(BufferedReader inputBufferedReader){
        this.fastq = inputBufferedReader;
    }

    /**
     *
     * @param inputFastq is the input text file in String
     */
    public void bufferInputFile(String inputFastq) {
        this.inputFastq = inputFastq;
    }

    /**
     *
     * @param outputFile
     */
    public void bufferOutputFile(String outputFile){
        /**
         *
         */
    }
}
