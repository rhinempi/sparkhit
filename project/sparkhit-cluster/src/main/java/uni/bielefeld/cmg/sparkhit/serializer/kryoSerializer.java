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
public class kryoSerializer implements ShKryoSerializer {

    private Kryo kryo = new Kryo();
    private Output output;
    private Input input;

    /**
     *
     * @param object
     * @param outFile
     */
    public void javaSerialization(Object object, String outFile){

    }

    /**
     *
     * @param inFile
     * @return
     */
    public Object javaDeSerialization(String inFile){
        return null;
    }

    public void setRegisterClass(Class myClass, int id){
        kryo.register(myClass, id);
    }

    public void writeObject(FileOutputStream fileOut, Object object){
        output = new Output(fileOut);
        kryo.writeObject(output, object);
        output.close();
    }

    public void kryoSerialization(Object object, String outFile, int registerId){
        Class myClass = object.getClass();
        setRegisterClass(myClass, registerId);

        ObjectFileOutput outputObject = new ObjectFileOutput();
        outputObject.setOutput(outFile, false);
        FileOutputStream fileOut = outputObject.getFileOutputStream();

        writeObject(fileOut, object);
    }

    public Object readObject(FileInputStream fileIn, Class myClass){
        input = new Input(fileIn);
        Object object = kryo.readObject(input, myClass);
        input.close();
        return object;
    }
    public Object kryoDeSerialization(Class myClass, String inFile){
        ObjectFileInput inputObject = new ObjectFileInput();
        inputObject.setInput(inFile);
        FileInputStream fileIn = inputObject.getInputStream();

        return readObject(fileIn, myClass);
    }
}
