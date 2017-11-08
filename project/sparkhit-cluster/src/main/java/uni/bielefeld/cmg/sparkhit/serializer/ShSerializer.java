package uni.bielefeld.cmg.sparkhit.serializer;

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
 * This is an interface for different serializers.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public interface ShSerializer {
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
     * @return an de-serialized object from a pre-serialized object.
     */
    Object kryoDeSerialization(Class myClass, String inFile);

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
     * @return an de-serialized object from a pre-serialized object.
     */
    Object javaDeSerialization(String inFile);
}
