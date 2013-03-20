wat-mining
==========

WAT (web archive transform) metadata mining

###About
A collection of pig scripts for performing web archive analysis tasks. The scripts rely on Apache Hadoop and Pig Latin and make use of the [archive-metadata-extractor](https://webarchive.jira.com/wiki/display/Iresearch/archive-metadata-extractor.jar) library.

###Files
* `/src/pig/` contains pig scripts that can be run against WAT files (or folders containing them), which can reside locally or on the hdfs file system.   
* `/src/resouces/sh` contains shell scripts that can be used to automate tasks like calling the wat extraction tool or displaying the result files generated by Hadoop.
* `/src/resouces/grunt` contains small example input files and pig statements useful for experimentation using pig's grunt shell. 

###Grunt shell
Apache Pig provides an interactive shell, called grunt, which provides a useful environment for experimenting with Pig programs. The shell is started locally using the command `pig -x local`. The pig script in `/src/resouces/grunt` provides a set of statements that can be used with grunt to read test input from a file and to perform a subsequent data analysis task.     

###Extracting WAT files
The [Web Archive Transformation (WAT)](https://webarchive.jira.com/wiki/display/Iresearch/Web+Archive+Transformation+%28WAT%29+Specification,+Utilities,+and+Usage+Overview) specification provides a structure for storing web archive metadata. WAT files can be easily extracted on the command-line from WARC files generated by the Heritrix web crawler, using the [archive-metadata-extractor](https://webarchive.jira.com/wiki/display/Iresearch/archive-metadata-extractor.jar) library. `extractor.sh` provides a simple shell script that generats wat files from a directory that contains a set of warc files. The archive-metadata-extractor can consume files via `file://` as well as `hdfs://` URIs. For files residing on hdfs, it is important to ensure that the WAT extractor has been compiled against the same Hadoop version that is running on the cluster. If WAT files are extracted locally they can be moved to hdfs using the hadoop file system shell commands.

###Executing the PIG scripts
The PIG scripts in `/src/pig/` can be executed on the cluster using the command `pig FILENAME` or be executed locally using `pig -x local FILENAME`. The input to both scripts can be a directory or a file which can reside locally or on hdfs. The default input is specified within the script using the default statement (e.g. `%default INPUT 'wat';`). The default values can also be overwritten using command-line parameters in the form of `pig {–param param_name = param_value}`. Outputs of the data analyis scripts are created by writing result data sets to the file system using the `STORE` statement. Depending on the execution environment (local or cluster) outputs are written to the local file system or to hdfs.

###Pig scripts
* `Allinks.pig` is a script to test for crawl completeness. It basically extracts the URLS for all crawled web pages and the destination URLs of all links contained within these web pages. The script then tries to find a crawled web site for every link. The result provides an indicator for crawl completeness and shows the number of crawled pages, contained links, and pages that have not been crawled. `eval.sh` is a simple testing script that outputs these results in aggregated form.

* `Prank.sh` uses the PageRank algorithm to calculate the relevance of crawled web resources. The script can be configured to do the calculation on different levels of granularity - like for example hosts or pages - based on adjusting a regular expression that extracts the relevant bits of the source URLs. The script also removes dangling pages (i.e. pages without outlinks to the crawled sites) before performing the page rank calculation. This is important to avoid a loss of the overall page rank weight (which must be 1) while calculating the page rank in muliple iterations. A core implementation of the used algorithm has been published [here](http://techblug.wordpress.com/2011/07/29/pagerank-implementation-in-pig/).   





   
