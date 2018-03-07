package uni.bielefeld.cmg.sparkhit.io;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.*;

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

public class FastqUnitBufferTest  extends TestCase {
    protected BufferedReader inputBuffer;

    @Test (expected = IOException.class)
    public void testFastqUnitBufferException() {
        FastqUnitBuffer fqbuffer = new FastqUnitBuffer(inputBuffer);
        try {
            fqbuffer.loadBufferedFastq();
        }catch(RuntimeException e){

        }
    }

    protected void setUp(){
        TextFileBufferInput inputfile = new TextFileBufferInput();
        inputfile.setInput("src/test/resources/fastq.fq");
        inputBuffer = inputfile.getBufferReader();
    }
}