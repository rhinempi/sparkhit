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
 # with this program. If not, see <http://www.gnu.org/licenses>.


# path
sparkbin="/root/spark/bin"
sparkhitlib="/mnt/software/sparkhit/lib/"
sparkhitpackage="/mnt/software/sparkhit/package/"

# spark metrics directory
mkdir -p /tmp/spark-events

# spark submit
time $sparkbin/spark-submit \
	--conf "spark.eventLog.enabled=true" \
	--conf "spark.task.cpus=32" \
	--driver-memory 15G \
	--executor-memory 57G \
	--class uni.bielefeld.cmg.sparkhit.main.MainOfParallelizer \
	$sparkhitlib/original-sparkhit-0.7.jar \
		-nodes 50 \
		-tool $sparkhitpackage/pipe-kallisto-local.sh \
		-toolparam " quant -i /mnt/reference/kallisto/transcript.index --single --plaintext -l 100 -s 20 -t 15 " \
		-outfile /mnt/sparkhit/kallisto \
		> sparkhit-kallisto.log 2> sparkhit-kallisto.err