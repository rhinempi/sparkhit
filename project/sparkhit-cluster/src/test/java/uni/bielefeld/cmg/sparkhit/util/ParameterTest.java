package uni.bielefeld.cmg.sparkhit.util;

import junit.framework.TestCase;
import org.apache.commons.cli.ParseException;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by Liren Huang on 07.11.17.
 *
 *      SparkHit
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * FragRec is free software: you can redistribute it and/or modify it
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