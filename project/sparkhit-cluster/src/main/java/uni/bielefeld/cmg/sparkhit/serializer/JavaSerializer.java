package uni.bielefeld.cmg.sparkhit.serializer;

import uni.bielefeld.cmg.sparkhit.io.ObjectFileInput;
import uni.bielefeld.cmg.sparkhit.io.ObjectFileOutput;
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
public class JavaSerializer /*implements ShJavaSerializer*/{

    private ObjectOutputStream out;
    private ObjectInputStream in;
    private InfoDumper info = new InfoDumper();

    public void writeOutObject(Object object){
        try{
            out.writeObject(object);
            out.close();
        }catch(IOException e){
            info.readIOException(e);
            info.screenDump();
            System.exit(0);
        }
    }

    public void javaSerialization(Object object, String outFile){
        ObjectFileOutput objectOut = new ObjectFileOutput();
        objectOut.setOutput(outFile, false);
        this.out = objectOut.getObjectOutputStream();
        writeOutObject(object);
    }

    public Object readInObject(){
        try {
            return in.readObject();
        } catch (IOException e) {
            info.readIOException(e);
            info.screenDump();
            System.exit(0);
        } catch (ClassNotFoundException e) {
            info.readClassNotFoundException(e);
            info.screenDump();
            System.exit(0);
        }

        return null;
    }

    public Object javaDeSerialization(String inFile){
        ObjectFileInput objectIn = new ObjectFileInput();
        objectIn.setInput(inFile);
        this.in = objectIn.getInputObjectStream();
        Object inObject = readInObject();

        return inObject;
    }

    public void kryoSerialization(Object object, String outFile){

    }

    public Object kryoDeSerialization(String inFile){
        return null;
    }
}
