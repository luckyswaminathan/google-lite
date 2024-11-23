to use search:
- make sure crawler and pagerank have been run
 make sure tfidf.jar exists in root folder. this can be built from jobs/TFIDF.java. 
- Run HW9 Test with arg tfidf to generate pt-tf and pt-idf tables
- Run search/Search.java  with argument of KVS coordinator: `java -cp src cis5550.search.Search localhost:8000`. This should take < 1s
- To modify the demo search string, amend Search.java and recompile
