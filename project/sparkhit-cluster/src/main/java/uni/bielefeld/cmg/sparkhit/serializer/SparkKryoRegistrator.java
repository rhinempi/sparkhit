package uni.bielefeld.cmg.sparkhit.serializer;

import org.apache.spark.serializer.KryoRegistrator;
import uni.bielefeld.cmg.sparkhit.matrix.ScoreMatrix;
import uni.bielefeld.cmg.sparkhit.reference.RefStructBuilder;
import uni.bielefeld.cmg.sparkhit.struct.*;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import com.esotericsoftware.kryo.Kryo;

import java.util.ArrayList;
//import org.apache.spark.serializer.KryoRegistrator;

/**
 * Created by Liren Huang on 13/01/16.
 * <p/>
 * SparkHit
 * <p/>
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 * <p/>
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 * <p/>
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */
public class SparkKryoRegistrator implements KryoRegistrator {

    public SparkKryoRegistrator (){
        /**
         * Registing all data structure for kryo serialization
         */
    }

    /**
     *
     * @param kryo
     */
    public void registerClasses(Kryo kryo){
        kryo.register(RefStructBuilder.class);
        kryo.register(ScoreMatrix.class);
        kryo.register(BinaryBlock.class);
        kryo.register(Block.class);
        kryo.register(RefTitle.class);
        kryo.register(KmerLoc.class);
        kryo.register(DefaultParam.class);
        kryo.register(AlignmentParameter.class);
    }
}