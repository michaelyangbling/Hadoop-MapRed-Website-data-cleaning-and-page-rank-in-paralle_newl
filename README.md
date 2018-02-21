# wiki-page-rank-in-parallel
This uses  2006 Wikipedia website data of 100GB before compression. The compressed file is in https://drive.google.com/drive/folders/1IIySfwwyvup2cy2bP4BfFaTYoFUSbWlK
1. code in Hadoop MapReduce and do parallel data cleaning and transferring to Graph( adjacent list )
2. code  in Hadoop MapReduce: parallel iterative page rank algorithm, considering dangling nodes etc.
3. code parallel algorithm to sort and output top 100 pages
All the above jobs can be chained and run in Amazon Web services EMR.
Project Still ongoing, just finished step 1
