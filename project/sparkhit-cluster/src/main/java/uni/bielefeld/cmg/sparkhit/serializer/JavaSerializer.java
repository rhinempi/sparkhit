package uni.bielefeld.cmg.sparkhit.serializer;

import uni.bielefeld.cmg.sparkhit.io.ObjectFileInput;
import uni.bielefeld.cmg.sparkhit.io.ObjectFileOutput;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.*;

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

/**
 * Returns an object for serializing objects using default Java serializer.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class JavaSerializer /*implements ShJavaSerializer*/{

    private ObjectOutputStream out;
    private ObjectInputStream in;
    private InfoDumper info = new InfoDumper();

    /**
     * This method writes object bits into output stream.
     *
     * @param object the object to be write out.
     */
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

    /**
     * This method serializes an object to an assigned file.
     *
     * @param object the object to be serialized.
     * @param outFile the full path to which the serialized bits store.
     */
    public void javaSerialization(Object object, String outFile){
        ObjectFileOutput objectOut = new ObjectFileOutput();
        objectOut.setOutput(outFile, false);
        this.out = objectOut.getObjectOutputStream();
        writeOutObject(object);
    }

    /**
     * Return the input object from input deserialization stream.
     *
     * @return the object that is been deserialized.
     */
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

    /**
     * This method deserializes an input file into an object.
     *
     * @param inFile the full path of an input file.
     * @return the object that is been deserialized.
     */
    public Object javaDeSerialization(String inFile){
        ObjectFileInput objectIn = new ObjectFileInput();
        objectIn.setInput(inFile);
        this.in = objectIn.getInputObjectStream();
        Object inObject = readInObject();

        return inObject;
    }

    /**
     * This method serializes an object using kryo serializer.
     *
     * @param object the object to be serialized.
     * @param outFile the full path to which the serialized bits store.
     */
    public void kryoSerialization(Object object, String outFile){

    }

    /**
     * Return the input object from input deserialization stream.
     *
     * @param inFile the full path of an input file.
     * @return the object that is been deserialized.
     */
    public Object kryoDeSerialization(String inFile){
        return null;
    }
}
