package uni.bielefeld.cmg.sparkhit.hadoop.decodec.util;

import java.io.Serializable;
import java.util.regex.Pattern;

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
public class DefaultParam implements Serializable{

    public DefaultParam (){
        /**
         * This is a class of data structure which stores the default parameters
         */
    }

    public String mightyName = "SparkHit";
    public boolean bz2 = false;
    public boolean gz = false;
    public String inputFqPath;              // undefaultable
    public String outputPath;
    public Integer outputFormat=0;
    public boolean overwrite = false;
    public long splitsize = 0;


}
