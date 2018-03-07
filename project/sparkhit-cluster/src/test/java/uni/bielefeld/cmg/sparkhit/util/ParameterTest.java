package uni.bielefeld.cmg.sparkhit.util;

import junit.framework.TestCase;
import org.apache.commons.cli.ParseException;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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


public class ParameterTest extends TestCase {
    String[] arguments;
    String input;
    String inputMarker = "-fastq";
    String output;
    String outputMarker = "-outfile";
    String kmer;
    String kmerMarker = "-kmer";
    String overlap;
    String overlapMarker = "-overlap";
    String minlength;
    String minlengthMarker = "-minlength";
    String attemp;
    String attempMarker = "-attempts";
    String hits;
    String hitsMarker = "-hits";
    String strand;
    String strandMarker = "-strand";
    String inputref;
    String inputrefMaker = "-reference";
    Parameter parameter;
    DefaultParam param;

    @Test (expected = RuntimeException.class)
    public void testInputFileExeption() {
        setWrongInput();

        try {
            parameter = new Parameter(arguments);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            param = parameter.importCommandLine();
        }catch (RuntimeException e){

        }

    }

    public void setWrongInput(){
        inputref = "/mnt/data/reference.fq"; // a wrong input reference fasta file
        arguments = new String[]{
                inputrefMaker, inputref,
                inputMarker, input,
                outputMarker, output,
                kmerMarker, kmer,
                overlapMarker, overlap,
                minlengthMarker, minlength,
                attempMarker, attemp,
                hitsMarker, hits,
                strandMarker, strand,
        };
    }

    @Test (expected = RuntimeException.class)
    public void testOutputFileExeption() {
        setWrongOutput();

        try {
            parameter = new Parameter(arguments);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            param = parameter.importCommandLine();
        }catch (RuntimeException e){

        }

    }

    public void setWrongOutput(){

        arguments = new String[]{
                inputrefMaker, inputref,
                inputMarker, input,
                outputMarker, //output, output file not set
                kmerMarker, kmer,
                overlapMarker, overlap,
                minlengthMarker, minlength,
                attempMarker, attemp,
                hitsMarker, hits,
                strandMarker, strand,
        };
    }

    @Test (expected = RuntimeException.class)
    public void testOverlapExeption() {
        setWrongOverlap();

        try {
            parameter = new Parameter(arguments);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            param = parameter.importCommandLine();
        }catch (RuntimeException e){

        }

    }

    public void setWrongOverlap(){

        overlap = "16";  // set overlap to a large size that is not accepted by the program

        arguments = new String[]{
                inputrefMaker, inputref,
                inputMarker, input,
                outputMarker, output,
                kmerMarker, kmer,
                overlapMarker, overlap,
                minlengthMarker, minlength,
                attempMarker, attemp,
                hitsMarker, hits,
                strandMarker, strand,
        };
    }

    @Test (expected = RuntimeException.class)
    public void testKmerExeption() {
        setWrongKmer();

        try {
            parameter = new Parameter(arguments);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            param = parameter.importCommandLine();
        }catch (RuntimeException e){

        }

    }

    public void setWrongKmer(){

        kmer = "7";  // set kmer to a small size that is not accepted by the program

        arguments = new String[]{
                inputrefMaker, inputref,
                inputMarker, input,
                outputMarker, output,
                kmerMarker, kmer,
                overlapMarker, overlap,
                minlengthMarker, minlength,
                attempMarker, attemp,
                hitsMarker, hits,
                strandMarker, strand,
        };
    }

    @Test (expected = RuntimeException.class)
    public void testMinLengthExeption() {
        setWrongMinLength();

        try {
            parameter = new Parameter(arguments);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            param = parameter.importCommandLine();
        }catch (RuntimeException e){

        }

    }

    public void setWrongMinLength(){

        minlength = "-1";  // set the minimum read length to less than 0 that is not accepted by the program

        arguments = new String[]{
                inputrefMaker, inputref,
                inputMarker, input,
                outputMarker, output,
                kmerMarker, kmer,
                overlapMarker, overlap,
                minlengthMarker, minlength,
                attempMarker, attemp,
                hitsMarker, hits,
                strandMarker, strand,
        };
    }

    @Test (expected = RuntimeException.class)
    public void testAttemptsExeption() {
        setWrongAttempts();

        try {
            parameter = new Parameter(arguments);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            param = parameter.importCommandLine();
        }catch (RuntimeException e){

        }

    }

    public void setWrongAttempts(){

        attemp = "0";  // set attemp to 0 that is not accepted by the program

        arguments = new String[]{
                inputrefMaker, inputref,
                inputMarker, input,
                outputMarker, output,
                kmerMarker, kmer,
                overlapMarker, overlap,
                minlengthMarker, minlength,
                attempMarker, attemp,
                hitsMarker, hits,
                strandMarker, strand,
        };
    }

    @Test (expected = RuntimeException.class)
    public void testHitsExeption() {
        setWrongHits();

        try {
            parameter = new Parameter(arguments);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            param = parameter.importCommandLine();
        }catch (RuntimeException e){

        }

    }

    public void setWrongHits(){

        hits = "-1";  // set hits to minus that is not accepted by the program

        arguments = new String[]{
                inputrefMaker, inputref,
                inputMarker, input,
                outputMarker, output,
                kmerMarker, kmer,
                overlapMarker, overlap,
                minlengthMarker, minlength,
                attempMarker, attemp,
                hitsMarker, hits,
                strandMarker, strand,
        };
    }

    @Test (expected = RuntimeException.class)
    public void testStrandExeption() {
        setWrongStrand();

        try {
            parameter = new Parameter(arguments);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            param = parameter.importCommandLine();
        }catch (RuntimeException e){

        }

    }

    public void setWrongStrand(){

        strand = "4";  // set hits to minus that is not accepted by the program

        arguments = new String[]{
                inputrefMaker, inputref,
                inputMarker, input,
                outputMarker, output,
                kmerMarker, kmer,
                overlapMarker, overlap,
                minlengthMarker, minlength,
                attempMarker, attemp,
                hitsMarker, hits,
                strandMarker, strand,
        };
    }

    protected void setUp(){
        input = "src/test/resources/fastq.fq";
        inputref="src/test/resources/reference.fa";
        output="result";
        kmer= "12";
        overlap = "8";
        minlength = "1";
        attemp = "1";
        hits = "1";
        strand = "0";
    }
}