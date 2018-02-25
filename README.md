# Hadoop-MapRed-Website-data-cleaning-and-page-rank-in-paralle_new

wiki-page-rank-in-parallel

This uses 2006 Wikipedia website data of 100GB before compression. 

The compressed file is in https://drive.google.com/drive/folders/1IIySfwwyvup2cy2bP4BfFaTYoFUSbWlK

The one named ..simple.. html is for local standalone debug

1. code in Hadoop MapReduce and do parallel data cleaning and transferring to Graph( adjacent list )
2. code in Hadoop MapReduce: parallel iterative page rank algorithm, considering dangling nodes etc. Give PageRank file with adjacent list and corresonding rank value

PageRank Formula:

with probability of jumping to a website:0.15, probability of following a link: 0.85 
here I iterate 10 hadoop MapReduce jobs to run my PageRank code
3. code parallel algorithm to sort and output top 100 pages All the above jobs can be chained and run in Amazon Web services EMR.
Project alomost done, have finished step 1,2 to generate the page rank, you can use make.mk to succesfully run in AWS, takes less than one hour when using 11 m4.large machine(set 8 reducers )
