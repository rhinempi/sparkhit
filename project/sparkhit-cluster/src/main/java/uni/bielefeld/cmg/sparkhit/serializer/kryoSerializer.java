package uni.bielefeld.cmg.sparkhit.serializer;

import com.esotericsoftware.kryo.io.Input;
import uni.bielefeld.cmg.sparkhit.io.ObjectFileInput;
import uni.bielefeld.cmg.sparkhit.io.ObjectFileOutput;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.FileInputStream;
import java.io.FileOutputStream;

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
