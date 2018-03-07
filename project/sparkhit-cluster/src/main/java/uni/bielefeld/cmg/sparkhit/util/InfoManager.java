package uni.bielefeld.cmg.sparkhit.util;

import java.io.FileNotFoundException;
import java.io.IOException;

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
 * This is an interface for managing program information. All classes formulate
 * and log the information produced by the source code.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public interface InfoManager {
    /**
     *
     */

    /**
     * This is an abstract method for recording specified messages.
     *
     * @param m the message that is going to be processed.
     */
    void readMessage(String m);

    /**
     * This is an abstract method for recording IOException messages.
     *
     * @param e the IOException message that is going to be processed.
     */
    void readIOException(IOException e);

    /**
     * This is an abstract method for recording FileNotFoundException messages.
     *
     * @param e the FileNotFoundException message that is going to be processed.
     */
    void readFileNotFoundException(FileNotFoundException e);

    /**
     * This is an abstract method for recording ClassNotFoundException messages.
     *
     * @param e the ClassNotFoundException message that is going to be processed.
     */
    void readClassNotFoundException(ClassNotFoundException e);

}
