package uni.bielefeld.cmg.sparkhit.serializer;

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
 * This is an interface for kryo serializers.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public interface ShKryoSerializer extends ShSerializer {

    /**
     * This is an abstract method for default Java serialization.
     *
     * @param object the object to be serialized.
     * @param outFile the output file that stores serialized data.
     */
    void javaSerialization(Object object, String outFile);

    /**
     * This is an abstract method for default Java de-serialization.
     *
     * @param inFile the full path of the pre-serialized object file.
     * @return
     */
    Object javaDeSerialization(String inFile);

    /**
     * This is an abstract method for kryo serialization.
     *
     * @param object the object to be serialized.
     * @param outFile the output file that stores serialized data.
     * @param registerId an register id for kryo serializer.
     */
    void kryoSerialization(Object object, String outFile, int registerId);

    /**
     * This is an abstract method for kryo de-serialization.
     *
     * @param myClass the class name that is used for de-serialization.
     * @param inFile the full path of the pre-serialized object file.
     * @return
     */
    Object kryoDeSerialization(Class myClass, String inFile);
}
