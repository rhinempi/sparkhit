#! /bin/bash

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
Tpath="/mnt/software/sparkhit/lib/"
Tpath2="/mnt/software/sparkhit/package/samtools/"
reference="/mnt/reference/9311/"

# command

while read line
do
	java -jar $Tpath/hadoop-bam-7.1.0-jar-with-dependencies.jar view $line |$Tpath2/samtools mpileup -uvf $reference/9311.fa /dev/stdin |perl -ne '$b=$_;@a=split /\t/,$b;print STDOUT $b if $a[4]=~/\,/ || $a[0]=~/^\#/'|perl $Tpath2/vcfutils.pl varFilter -d 5 /dev/stdin 2>$reference/mpileup.err
done 

