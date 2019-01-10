#!/usr/bin/env bash
#Created by rhinempi on 23/01/16.
 #
 #      SparkHit
 #
 # Copyright (c) 2015-2015
 #      Liren Huang      <huanglr at cebitec.uni-bielefeld.de>
 #
 # SparkHit is free software: you can redistribute it and/or modify it
 # under the terms of the GNU General Public License as published by the Free
 # Software Foundation, either version 3 of the License, or (at your option)
 # any later version.
 #
 # This program is distributed in the hope that it will be useful, but WITHOUT
 # ANY WARRANTY; Without even the implied warranty of MERCHANTABILITY or
 # FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 # more detail.
 #
 # You should have received a copy of the GNU General Public License along
 # with this program. If not, see <http://www.gnu.org/licenses>


 # run the following command in the sparkhit root directory


./bin/sparkhit reductioner \
    --conf spark.driver.maxResultSize=10g \
    --conf spark.kryoserializer.buffer.max=2000m \
    --driver-memory 8G --executor-memory 8G \
    -vcf ./example/1kgenome.vcf.tar.gz \
    -outfile ./example/pca \
    -column 10-1500 \
    -cache