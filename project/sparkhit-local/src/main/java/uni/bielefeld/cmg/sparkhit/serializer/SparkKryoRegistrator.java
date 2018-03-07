package uni.bielefeld.cmg.sparkhit.serializer;

import uni.bielefeld.cmg.sparkhit.matrix.ScoreMatrix;
import uni.bielefeld.cmg.sparkhit.struct.AlignmentParameter;
import uni.bielefeld.cmg.sparkhit.struct.BinaryBlock;
import uni.bielefeld.cmg.sparkhit.struct.KmerLoc;
import uni.bielefeld.cmg.sparkhit.struct.RefTitle;
import uni.bielefeld.cmg.sparkhit.util.DefaultParam;
import com.esotericsoftware.kryo.Kryo;
//import org.apache.spark.serializer.KryoRegistrator;

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
public class SparkKryoRegistrator /*implements KryoRegistrator */{

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
        //kryo.register(RefSeq.class);
        kryo.register(ScoreMatrix.class);
        kryo.register(BinaryBlock.class);
        kryo.register(RefTitle.class);
        kryo.register(KmerLoc.class);
        kryo.register(DefaultParam.class);
        kryo.register(AlignmentParameter.class);
    }
}
