# Hadoop-MapRed-Website-data-cleaning-and-page-rank-in-parallel


This uses 2006 Wikipedia website data of 100GB before compression. 

The compressed file is in https://drive.google.com/drive/folders/1IIySfwwyvup2cy2bP4BfFaTYoFUSbWlK

The one named ..simple.. html is for local standalone debug.

step1,2: code in Hadoop MapReduce and do parallel HTML parsing ,data cleaning and transferring to Graph( adjacent list ).
step3. code in Hadoop MapReduce: parallel iterative page rank algorithm, considering dangling nodes etc. Give PageRank file with adjacent list and corresonding rank value.

PageRank Formula is in page rank formula.pdf.
Here I take  dandling nodes into consideration.
Here I use probability of jumping to a website:0.15, probability of following a link: 0.85.
here I iterate 10 hadoop MapReduce jobs to run my PageRank code

step4. code parallel algorithm to sort and output top 100 pages .All the above jobs can be chained and run in Amazon Web services EMR.

you can use make.mk to succesfully run in AWS, 

1.build the jar file.
In a maven project with this pom.xml, cd to the project directory,  in command line: mvn clean install 

2.Amazon Web Services Run.
Make sure my make.mk makefile is in directory,in command line: make -f make.mk awsrun

It takes 58min when using 11  m4.large( a cheap machine on AWS)  machines (set 8 reduce tasks )

It takes 36min when using 20 m4.large machines (set 19 reduce tasks ).
Web parsing and Data-cleaning, each of pageRank iterations and the final top-K algorithms all show about 2 times speedup.

So even though the data is much bigger, you can simply use more machines(workers) to bring even more speedup for this data pipeline I built. 
