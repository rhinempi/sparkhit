package uni.bielefeld.cmg.sparkhit.pipeline;


import uni.bielefeld.cmg.sparkhit.util.DefaultParam;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;

/**
 * Created by rhinempi on 23/01/16.
 *
 *      SparkHit
 *
 * Copyright (c) 2015-2015
 * Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
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
 */


/**
 * This is an interface for pipelines of each Sparkhit application.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public interface Pipeline {

    /**
     * Sets correspond parameter.
     *
     * @param param {@link DefaultParam} is the object for command line parameters.
     */
    void setParameter(DefaultParam param);

    /**
     * Sets input buffer reader.
     *
     * @param InputRead a {@link BufferedReader} to read input data.
     */
    void setInput(BufferedReader InputRead);

    /**
     * Sets output buffer writer.
     *
     * @param OutputWrite a {@link BufferedWriter} to write to an output file.
     */
    void setOutput(BufferedWriter OutputWrite);
}
