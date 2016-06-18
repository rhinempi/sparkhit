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



# parameter of the shell script
PARAM=$1
Tpath="/mnt/software/sparkhit/package/kraken/"

# command

# step 1 download kraken database
mkdir -p /mnt/reference/kraken
rm -rf /mnt/reference/kraken/miniKraken.tar.gz
wget https://s3-eu-west-1.amazonaws.com/sparkhit-distributed-dataset/miniKraken/miniKraken.tar.gz -P /mnt/reference/kraken/
# step 2 decompress kraken database
tar zxvf /mnt/reference/kraken/miniKraken.tar.gz -C /mnt/reference/kraken/
# step 3 kraken profiler
$Tpath/kraken $PARAM --db /mnt/reference/kraken/minikraken_20141208/ /dev/stdin 
