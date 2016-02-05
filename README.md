# sparkhadooppatch
Jar File containing patched Hadoop-MapReduce-Client 2.7.2 with DirectOutputCommitter

This allows direct saving to S3 without creating a _temporary directory on S3 first.
Add jar file to Spark Jar Path and this entry to spark-defaults.conf

spark.hadoop.mapred.output.committer.class org.apache.hadoop.mapred.DirectOutputCommitter

Hadoop 2.7.2

Thanks to Databricks for their Scala version

https://gist.github.com/aarondav/c513916e72101bbe14ec