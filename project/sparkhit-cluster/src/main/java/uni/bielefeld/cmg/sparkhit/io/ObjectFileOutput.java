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
public class ObjectFileOutput implements OutputFileManager {
    private String path;
    private File outputPath;
    public FileOutputStream outputFileStream;
    public ObjectOutputStream outputObjectStream;
    private InfoDumper info = new InfoDumper();

    public FileOutputStream getFileOutputStream(){
        return outputFileStream;
    }

    public ObjectOutputStream getObjectOutputStream(){
        return outputObjectStream;
    }

    private void setObjectOutputStream(){
        try {
            this.outputObjectStream = new ObjectOutputStream(outputFileStream);
        } catch (IOException e) {
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    private void setFileOutputStream(boolean overwrite){
        try{
            this.outputFileStream = new FileOutputStream(outputPath, overwrite);
        }catch(FileNotFoundException e){
            e.fillInStackTrace();
            System.exit(0);
        }
    }

    private void setOutputFile(){
        this.outputPath = new File(path);
    }

    public void setOutput(String outputFile, boolean overwrite){
        this.path = outputFile;
        setOutputFile();
        setFileOutputStream(overwrite);
        setObjectOutputStream();
    }

    public void bufferOutputFile(String outputFile){
        this.path = outputFile;
    }

    public void bufferInputFile(String inputFile){

    }
}
