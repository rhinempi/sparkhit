package uni.bielefeld.cmg.sparkhit.util;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by Liren Huang on 13/01/16.
 *
 * SparkHit
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
