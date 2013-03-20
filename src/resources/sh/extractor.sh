#!/bin/bash

# a script to run archive-metadata-extractor.jar against multiple warcs

#extractor_jar=archive-metadata-extractor-20110430.jar
extractor_jar=archive-meta-extractor-20110609.jar
cmd=$1
dir=$2
#hadoop dfs -ls hdfs://master:8020/user/rainer/warcs/WEB-20130125064209488-00014-31940~s3scape01~8083.warc.gz
hdfs_uri="hdfs://master"
wwarcgz="/*.warc.gz"
watgz="wat.gz"

usage () {
   echo "Usage: $0 <cmd> <warc_dir>"
   echo "applies a command to all warc.gz files in a specified directory"

   echo "warc_dir: local path, HTTP or HDFS URL to an arc, warc, arc.gz, or warc.gz"
   
   echo "cmd:"
   echo "-wat generates WAT (Web Archive Transformation) compressed records"
}

if [ -z "$1" -o -z "$2" ]; then
   echo "cmd and/or target dir required"
   usage
   exit 1
fi

# extract wat files from a file URI
if [ "$cmd" = "-wat" ]; then
   files=`ls -l $dir$wwarcgz | awk '$5>0 {print$9}'`
   for file in $files; do
      #remove absolute file path
      resfile=`ls $file | awk -F/ '{print $NF}'`
      length=`expr length $resfile`
      let "length -= 7"
      resfile=${resfile:0:length}$watgz
      echo "$resfile"
      return=`java -jar $extractor_jar $cmd $file >> $resfile`
   done
fi

# extract wat files from hdfs URI
# TODO automatically detect hdfs URIs
# TODO ensure the WAT extractor has been compiled against the same Hadoop version that is running on the cluster. 
if [ "$cmd" = "-wath" ]; then
	 cmd="-wat"
   files=`hadoop dfs -ls warcs | awk '$5>0 {print$8}'`
   for file in $files; do
      echo "$hdfs_uri$file"
      return=`java -jar $extractor_jar $cmd $hdfs_uri$file`
   done
fi








