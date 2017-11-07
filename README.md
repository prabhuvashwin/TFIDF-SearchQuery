ITCS 6190 - Cloud Computing for Data Analysis - Assignment 2
—————————————————————————————————————————————————————————————

This is a read me document for the assignment 2 and explains all the files involved.

The input files used for this dataset is the Canterbury Corpus. It consists of 8 files of different formats.

While running the program, the output gets saved in the following manner:
Consider OUTPUT_VALUE is the output location passed by the user, then
DocWordCount Output location - OUTPUT_VALUE/docwordcount
TermFrequency Output location - OUTPUT_VALUE/tf
TFIDF Output location - OUTPUT_VALUE/tfidf
Search Output location - OUTPUT_VALUE/search

-----------------------------------------------------------------------------------------
COMMANDS used to the run the below files is same as that provided in the ClusterExample.pdf are also given below:

- Copy all the source code files onto the cluster at path ~/assignment2/build/org/myorg/
- Copy all the input files onto the hdfs file system at path /user/<username>/wordcount/input
- Compile the source code files using the command - $ javac -cp /opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/* <FILENAME>.java -d build -Xlint
- Create jar file using the command - $ jar -cvf <filename>.jar -C build/ .
- Execute the jar file using - $ hadoop jar <filename>.jar org.myorg.<ObjectName> INPUT_PATH OUTPUT_PATH
-----------------------------------------------------------------------------------------

The assignment has four sections as mentioned below:

1) DocWordCount - The code for DocWordCount is present in DocWordCount.java. The output after running the DocWordCount on Canterbury corpus is available in output/docwordcount.out

2) TermFrequency - The code for TermFrequency is present in TermFrequency. This file is not executed directly and is a part of chaining in TFIDF.java. The output after running the TermFrequency on Canterbury corpus is available in output/TermFrequency.out

3) TFIDF - The code for TFIDF is present in TFIDF.java. The output after running the TFIDF on Canterbury corpus is available in output/TFIDF.out

4) Search - The code for Search is present in Search.java. Two queries are run using Search.java. First one, where the query passed is “computer science”, and the output for this query is available in output/query1.out. Second one, where the query passed is “data analysis”, and the output for this query is available in output/query2.out.