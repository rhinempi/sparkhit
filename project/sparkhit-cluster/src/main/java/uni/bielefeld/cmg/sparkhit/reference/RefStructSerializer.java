package uni.bielefeld.cmg.sparkhit.reference;


import uni.bielefeld.cmg.sparkhit.io.TextFileBufferInput;
import uni.bielefeld.cmg.sparkhit.io.TextFileBufferOutput;
import uni.bielefeld.cmg.sparkhit.serializer.JavaSerializer;
import uni.bielefeld.cmg.sparkhit.serializer.kryoSerializer;
import uni.bielefeld.cmg.sparkhit.struct.BinaryBlock;
import uni.bielefeld.cmg.sparkhit.struct.Block;
import uni.bielefeld.cmg.sparkhit.struct.KmerLoc;
import uni.bielefeld.cmg.sparkhit.struct.RefTitle;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import uni.bielefeld.cmg.sparkhit.util.InfoDumper;

import java.io.*;
import java.sql.Ref;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Liren Huang on 17/02/16.
 *
 *      spark-hit_standalone
 *
 * Copyright (c) 2015-2015
 *      Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 * 
 * spark-hit_standalone is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more detail.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses>.
 *
 */

/**
 * Returns an object for serializing reference indices. This class is used
 * in local mode only. For cluster mode, Spark kryo serialization function
 * is used to broadcast objects into each worker nodes.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class RefStructSerializer implements RefSerializer, Serializable {
    private DefaultParam param;
    private kryoSerializer kSerializer = new kryoSerializer();
    private JavaSerializer jSerializer = new JavaSerializer();
    private RefStructBuilder ref;
    private InfoDumper info = new InfoDumper();

    /**
     * This method sets the input parameters.
     *
     * @param param {@link DefaultParam}.
     */
    public void setParameter(DefaultParam param){
        this.param = param;
    }

    /**
     * This method passes the reference of the reference index to this class.
     *
     * @return {@link RefStructBuilder}.
     */
    public RefStructBuilder getStruct(){
        return this.ref;
    }

    /**
     * This method de-serializes all reference indices using kryo serializer.
     */
    public void kryoDeserialization(){
        String bbl = param.inputFaPath + ".bbl";
        String blo = param.inputFaPath + ".blo";
        String tit = param.inputFaPath + ".tit";
        String ind = param.inputFaPath + ".ind";
        String met = param.inputFaPath + ".met";

        ref = new RefStructBuilder();
        ref.setParameter(param);

        File BBListFile = new File(bbl);
        if (BBListFile.exists()){
            ref.BBList = (List<BinaryBlock>) kSerializer.kryoDeSerialization(ArrayList.class, bbl);
        }else{
            info.readMessage("reference data index : " + bbl + " is missing");
            info.screenDump();
        }

        File blockFile = new File(blo);
        if (blockFile.exists()){
            ref.block = (List<Block>) kSerializer.kryoDeSerialization(ArrayList.class, blo);
        }else{
            info.readMessage("reference data index : " + blo + " is missing");
            info.screenDump();
        }

        File titleFile = new File(tit);
        if (titleFile.exists()){
            ref.title = (List<RefTitle>) kSerializer.kryoDeSerialization(ArrayList.class, tit);
        }else{
            info.readMessage("reference data index : " + tit + " is missing");
            info.screenDump();
        }

        File indexFile = new File(ind);
        if (indexFile.exists()){
            ref.index = (KmerLoc[]) kSerializer.kryoDeSerialization(KmerLoc[].class, ind);
        }else{
            info.readMessage("reference data index : " + ind + " is missing");
            info.screenDump();
        }

        File metaFile = new File(met);
        if (metaFile.exists()){
            readMetaData(met);
        }else{
            info.readMessage("reference data index : " + met + " is missing");
            info.screenDump();
        }

    }

    /**
     * This method serializes reference indices using kryo serializer.
     */
    public void kryoSerialization() {
        String bbl = param.inputFaPath + ".bbl";
        String blo = param.inputFaPath + ".blo";
        String tit = param.inputFaPath + ".tit";
        String ind = param.inputFaPath + ".ind";
        String met = param.inputFaPath + ".met";

        kSerializer.kryoSerialization(ref.BBList, bbl, 0);
        kSerializer.kryoSerialization(ref.block, blo, 0);
        kSerializer.kryoSerialization(ref.title, tit, 0);
        kSerializer.kryoSerialization(ref.index, ind, 1);
        putMetaData(met);

    }

    /**
     * This method de-serializes reference indices using default Java serializer.
     */
    public void javaDeSerialization(){
        String bbl = param.inputFaPath + ".bbl";
        String blo = param.inputFaPath + ".blo";
        String tit = param.inputFaPath + ".tit";
        String ind = param.inputFaPath + ".ind";
        String met = param.inputFaPath + ".met";

        ref = new RefStructBuilder();
        ref.setParameter(param);

        File BBListFile = new File(bbl);
        if (BBListFile.exists()){
            ref.BBList = (List<BinaryBlock>) jSerializer.javaDeSerialization(bbl);
        }else{
            info.readMessage("reference data index : " + bbl + " is missing");
            info.screenDump();
        }

        File blockFile = new File(blo);
        if (blockFile.exists()){
            ref.block = (List<Block>) jSerializer.javaDeSerialization(blo);
        }else{
            info.readMessage("reference data index : " + blo + " is missing");
            info.screenDump();
        }

        File titleFile = new File(tit);
        if (titleFile.exists()){
            ref.title = (List<RefTitle>) jSerializer.javaDeSerialization(tit);
        }else{
            info.readMessage("reference data index : " + tit + " is missing");
            info.screenDump();
        }

        File indexFile = new File(ind);
        if (indexFile.exists()){
            ref.index = (KmerLoc[]) jSerializer.javaDeSerialization(ind);
        }else{
            info.readMessage("reference data index : " + ind + " is missing");
            info.screenDump();
        }

        File metaFile = new File(met);
        if (metaFile.exists()){
            readMetaData(met);
        }else{
            info.readMessage("reference data index : " + met + " is missing");
            info.screenDump();
        }

    }

    /**
     * This method serializes reference indices using default Java serializer.
     */
    public void javaSerialization(){
        String bbl = param.inputFaPath + ".bbl";
        String blo = param.inputFaPath + ".blo";
        String tit = param.inputFaPath + ".tit";
        String ind = param.inputFaPath + ".ind";
        String met = param.inputFaPath + ".met";

        jSerializer.javaSerialization(ref.BBList, bbl);
        jSerializer.javaSerialization(ref.block, blo);
        jSerializer.javaSerialization(ref.title, tit);
        jSerializer.javaSerialization(ref.index, ind);
        putMetaData(met);

    }

    /**
     * This method loads pre-stored reference metadata into the program.
     *
     * @param met the full path of the .met file.
     */
    private void readMetaData(String met){
        TextFileBufferInput metaDataTextFileReader = new TextFileBufferInput();
        metaDataTextFileReader.setInput(met);
        BufferedReader metReader = metaDataTextFileReader.getBufferReader();

        try {
            String line;
            while((line = metReader.readLine()) !=null){
                if (line.startsWith("Reference total length : ")) {
                    ref.totalLength = Long.parseLong(line.split("\\t")[1]);
                } else if (line.startsWith("Reference total contig number : ")){
                    ref.totalNum = Integer.parseInt(line.split("\\t")[1]);
                }
            }
            metReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param met
     */
    private void putMetaData(String met){

        /* put total length and contig number in .met (meta) file */
        TextFileBufferOutput metaDataTextFileWriter = new TextFileBufferOutput();
        metaDataTextFileWriter.setOutput(met, false);
        BufferedWriter metWriter = metaDataTextFileWriter.getOutputBufferWriter();

        try {
            metWriter.write("Reference total length : \t" + ref.totalLength + "\n");
            metWriter.write("Reference total contig number : \t" + ref.totalNum + "\n");
            metWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setStruct(RefStructBuilder ref){
        this.ref = ref;
    }
}
