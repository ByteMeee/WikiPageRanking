# WikiPageRanking
##  Hadoop example - Page Ranking Wikipedia

This project is a sample how to work with Hadoop. ut containes 3 jobs to parse, calculate and order the page ranking
of a Wikipedia dump.

Requires:

- Hadoop cluster with HDFS.
- Maven.
- Wiki dump input file: http://dumps.wikimedia.org/nlwiki/latest/nlwiki-latest-pages-articles.xml.bz2.

You can run the jar in the `target` folder with 2 args: "input_pages result_folder".The `result_folder` will be created automatically and musn't exist at the start. 
