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
public interface ShJavaSerializer extends ShSerializer{

    /**
     * serialize objects with java default serializer
     *
     * @param object
     * @param outFile
     */
    void javaSerialization(Object object, String outFile);

    /**
     * de serialize objects with java default serialier
     *
     * @param inFile
     * @return
     */
    Object javaDeSerialization(String inFile);

    /**
     * this abstract method is invalid for this sub interface. Override it with null
     *
     * @param object
     * @param outFile
     */
    void kryoSerialization(Object object, String outFile);

    /**
     * this abstract method is invalid for this sub interface. Override it with null
     *
     * @param inFile
     * @return
     */
    Object kryoDeSerialization(String inFile);

}
