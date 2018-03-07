package uni.bielefeld.cmg.sparkhit.serializer;

import com.esotericsoftware.kryo.io.Input;
import uni.bielefeld.cmg.sparkhit.io.ObjectFileInput;
import uni.bielefeld.cmg.sparkhit.io.ObjectFileOutput;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.FileInputStream;
import java.io.FileOutputStream;

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
 * Returns an object for serializing objects using kryo serializer.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class kryoSerializer implements ShKryoSerializer {

    private Kryo kryo = new Kryo();
    private Output output;
    private Input input;

    /**
     * This method serializes an object to an assigned file.
     *
     * @param object the object to be serialized.
     * @param outFile the output file that stores serialized data.
     */
    public void javaSerialization(Object object, String outFile){

    }

    /**
     * Return the input object from input deserialization stream.
     *
     * @param inFile the full path of the pre-serialized object file.
     * @return
     */
    public Object javaDeSerialization(String inFile){
        return null;
    }

    /**
     * This method registers a class to kryo serializer.
     *
     * @param myClass the class to be registered by kryo register.
     * @param id an id for kryo registration.
     */
    public void setRegisterClass(Class myClass, int id){
        kryo.register(myClass, id);
    }

    /**
     * This method writes object bits into output stream.
     *
     * @param fileOut the output stream for the output file.
     * @param object the object to be write out.
     */
    public void writeObject(FileOutputStream fileOut, Object object){
        output = new Output(fileOut);
        kryo.writeObject(output, object);
        output.close();
    }

    /**
     * This method serializes an object to an assigned file using kryo serializer.
     *
     * @param object the object to be serialized.
     * @param outFile the output file that stores serialized data.
     * @param registerId an register id for kryo serializer.
     */
    public void kryoSerialization(Object object, String outFile, int registerId){
        Class myClass = object.getClass();
        setRegisterClass(myClass, registerId);

        ObjectFileOutput outputObject = new ObjectFileOutput();
        outputObject.setOutput(outFile, false);
        FileOutputStream fileOut = outputObject.getFileOutputStream();

        writeObject(fileOut, object);
    }

    /**
     * Return the input object from input deserialization stream.
     *
     * @param fileIn the input deserialization stream.
     * @param myClass the class name for an object that is going to be deserialized.
     * @return an object that is going to be deserialized.
     */
    public Object readObject(FileInputStream fileIn, Class myClass){
        input = new Input(fileIn);
        Object object = kryo.readObject(input, myClass);
        input.close();
        return object;
    }

    /**
     * Return the input object that is going to be deserialized.
     *
     * @param myClass the class name that is used for de-serialization.
     * @param inFile the full path of the pre-serialized object file.
     * @return
     */
    public Object kryoDeSerialization(Class myClass, String inFile){
        ObjectFileInput inputObject = new ObjectFileInput();
        inputObject.setInput(inFile);
        FileInputStream fileIn = inputObject.getInputStream();

        return readObject(fileIn, myClass);
    }
}
