package uni.bielefeld.cmg.sparkhit.io;


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
 * This is an interface for managing input files via different input
 * buffers.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public interface InputFileManager extends FileManager{
    /**
     * sub interface for managing input files
     */

    /**
     * This is an abstract method for buffering input stream of input files.
     *
     * @param inputFile the full path of an input file.
     */
    void bufferInputFile(String inputFile);

    /**
     * This is an abstract method for buffering output stream of output files.
     *
     * @param outputFile the full path of an output file.
     */
    void bufferOutputFile(String outputFile);
}
