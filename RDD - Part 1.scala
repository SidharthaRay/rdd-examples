// Databricks notebook source
// MAGIC %md
// MAGIC ** Example 1.1 - Word count example **

// COMMAND ----------

// Step 1: Upload a random text file to DBFS(DataBrick File System) and read that as an RDD[String]
val textRdd:RDD[String] = sc.textFile("/FileStore/tables/SampleText.txt")

// Step 2: Split each record/entry of the RDD through " " delimeter.
// A function that takes a String value as an parameter, splits it through " " and return the splitted words as an Array[String]
def strSplit(str:String): Array[String] = {
  return str.split(" ")
}
// Apply the above function to each record of the RDD and flatten it
val wordsRdd:RDD[String] = textRdd.flatMap(strSplit)

// Step 3: Covert each record or word into a (word, 1) pair
// A function that takes a word of type String value as an parameter and returns a (word, 1) pair
def wordPair(word: String): (String, Int) = {
  return (word, 1)
}
// Apply the above function to each record of the RDD and convert them into (word, 1) pair
val wordsCounterRdd:RDD[(String, Int)] = wordsRdd.map(wordPair)

// Step 4: Aggregate each key, i.e. the words, and calculate the sum of the counters, i.e. 1's 
// Calculating sum of counters for each key, i.e, word
def sum(accumulatedVal: Int, currentVal: Int): Int = {
  return (accumulatedVal + currentVal)
}
// Apply the above sum() function to calculate the frequency of each words.
val wordCountRdd:RDD[(String, Int)] = wordsCounterRdd.reduceByKey(sum)

wordCountRdd.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.2 - Creating RDD with parallelize() function **

// COMMAND ----------

val numRdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
numRdd.getNumPartitions

// COMMAND ----------

sc.defaultParallelism

// COMMAND ----------

val numRddParts: RDD[Array[Int]] = numRdd.glom()
numRddParts.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.3 - repartition() and coalesce() function **

// COMMAND ----------

val biggerRdd: RDD[Int] = sc.parallelize(1 to 1000, 8)
biggerRdd.take(3)

// COMMAND ----------

biggerRdd.getNumPartitions

// COMMAND ----------

biggerRdd.repartition(10).saveAsTextFile("/FileStore/tables/biggerRdd/ten")
sc.textFile("/FileStore/tables/biggerRdd/ten").getNumPartitions


// COMMAND ----------

biggerRdd.repartition(4).saveAsTextFile("dbfs:/FileStore/tables/biggerRdd/four")
sc.textFile("dbfs:/FileStore/tables/biggerRdd/four").getNumPartitions


// COMMAND ----------

biggerRdd.coalesce(1).saveAsTextFile("dbfs:/FileStore/tables/biggerRdd/coalesce")

// Homework: Is the number of records same in each of the partitions for repartition() and coalesce()?


// COMMAND ----------

val txnList: List[String] = List("txn01~1000", "txn02~2000", "txn03~1000", "txn04~2000")
val txnRdd: RDD[String] = sc.parallelize(txnList)

def txnSplit(txn: String): Array[String] = {
  return txn.split("~")
}
val splittedTxnRdd: RDD[Array[String]] = txnRdd.map(txnSplit)

def getTxnAmount(txn: Array[String]): Double = {
  return txn(1).toDouble
}
val txnAmtRdd: RDD[Double] = splittedTxnRdd.map(getTxnAmount)

def sum(amt1: Double, amt2: Double): Double = {
  return amt1 + amt2
}
txnAmtRdd.reduce(sum)


// COMMAND ----------

// Read "txn_fact.csv" are see the number of partitions, distinct "merchant_id" count


// COMMAND ----------

val biggerRdd: RDD[Int] = sc.parallelize(1 to 1000, 8)
biggerRdd.glom().map(rec => rec.size).collect()


// COMMAND ----------

biggerRdd.repartition(6).glom().map(rec => rec.size).collect()


// COMMAND ----------

biggerRdd.coalesce(10).glom().map(rec => rec.size).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.3 - Saving data as ObjectFile, SequenceFile and NewAPIHadoopFile **

// COMMAND ----------

val regRdd = sc.textFile("/FileStore/tables/KC_Extract_1_20171009.csv")
  .map(record => record.split("\\|", -1))

regRdd.take(3)


// COMMAND ----------

regRdd.saveAsTextFile("/FileStore/tables/Reg_v1/")
sc.textFile("dbfs:/FileStore/tables/Reg_v1/").take(3)


// COMMAND ----------

regRdd.map(record => record.mkString(",")).saveAsTextFile("/FileStore/tables/Reg_v2/")
sc.textFile("dbfs:/FileStore/tables/Reg_v2").take(3)


// COMMAND ----------

regRdd.saveAsObjectFile("/FileStore/tables/Reg_v3/")
sc.objectFile[Array[String]]("dbfs:/FileStore/tables/Reg_v3/part-00000").take(1)


// COMMAND ----------

import org.apache.hadoop.io.Text
regRdd.map(rec => (rec(4), rec(10))).saveAsSequenceFile("dbfs:/FileStore/tables/Reg_v4")
sc.sequenceFile("dbfs:/FileStore/tables/Reg_v4", classOf[Text], classOf[Text]).map(rec => rec.toString()).collect()


// COMMAND ----------

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

regRdd.map(record => (new Text(record(2)), new Text(record(10))))
  .saveAsNewAPIHadoopFile("dbfs:/FileStore/tables/Reg_v5/",
                         classOf[Text],
                         classOf[Text],
                         classOf[SequenceFileOutputFormat[Text, Text]])

sc.newAPIHadoopFile("/FileStore/tables/Reg_v5/part-r-00000",
                   classOf[SequenceFileInputFormat[Text, Text]],
                   classOf[Text],
                   classOf[Text])
  .map(rec => rec.toString()).take(3)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.4: Benefits of Laziness for Large - Scale Data **

// COMMAND ----------

val firstLogsWithErrors = logsRdd.filter(log => log.contains("2016") && log.contains("error"))
firstLogsWithErrors.take(5)


// COMMAND ----------

firstLogsWithErrors.count

// COMMAND ----------

val largeLogs = List(
  "2015-09-01 10:00:01|Error|Ac #3211001 ATW 10000 INR", 
  "2015-09-02 10:00:07|Info|Ac #3281001 ATW 11000 INR",
  "2015-10-01 10:00:09|error|Ac #3311001 AWT 10500 INR", 
  "2015-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR",
  "2016-09-01 10:00:01|info|Ac #3211001 AWT 5000 INR", 
  "2016-09-02 10:00:01|ERROR|Ac #3211001 AWT 10000 INR",
  "2016-10-01 10:00:01|error|Ac #3211001 AWT 8000 INR", 
  "2016-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR",
  "2016-12-01 10:00:01|Error|Ac #8211001 AWT 80000 INR", 
  "2016-12-02 10:00:01|error|Ac #9211001 AWT 90000 INR",
  "2016-12-10 10:00:01|error|Ac #3811001 AWT 15000 INR", 
  "2016-12-01 10:00:01|info|Ac #3219001 AWT 16000 INR"
)

val logsRdd = sc.parallelize(largeLogs)

logsRdd.filter(log => log.contains("2016-12")).count()


// COMMAND ----------

logsRdd.map(log => log.toLowerCase()).filter(log => log.contains("2016-12") && log.contains("error")).count()


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.5: Set Operations **

// COMMAND ----------

val etLog = List(
  "2017-10-13 10:00:00|RIL|830.00",
  "2017-10-13 10:00:10|MARUTI SUZUKI|8910.00",
  "2017-10-13 10:00:20|RIL|835.00",
  "2017-10-13 10:00:30|MARUTI SUZUKI|8890.00"
)
val mcLog = List(
  "2017-10-13 10:00:20|RIL|835.00",
  "2017-10-13 10:00:40|MARUTI SUZUKI|8870.00"
)
val etLogRDD = sc.parallelize(etLog)
val mcLogRDD = sc.parallelize(mcLog)


// COMMAND ----------

// Union - Share price example
println("Total number of stocks: " + etLogRDD.union(mcLogRDD).distinct().count())
etLogRDD.union(mcLogRDD).distinct().collect()


// COMMAND ----------

// Intersection
etLogRDD.intersection(mcLogRDD).collect()


// COMMAND ----------

// Set difference
etLogRDD.subtract(mcLogRDD).collect()


// COMMAND ----------

// Cartesian product
etLogRDD.cartesian(mcLogRDD).count()


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.6: Other Useful RDD Actions **

// COMMAND ----------

val numRDD = sc.parallelize(List.range(0, 10))
numRDD.takeSample(false, 5)
//numRDD.takeSample(true, 5)
//numRDD.takeSample(false, 5, 100)


// COMMAND ----------

sc.parallelize(List(10, 1, 2, 9, 3, 4, 5, 6, 7)).takeOrdered(6)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.7: Caching and Persistence **

// COMMAND ----------

import org.apache.spark.storage.StorageLevel
val lastYearsLogs = List(
  "2015-09-01 10:00:01|Error|Ac #3211001 ATW 10000 INR", 
  "2015-09-02 10:00:07|Info|Ac #3281001 ATW 11000 INR",
  "2015-10-01 10:00:09|error|Ac #3311001 AWT 10500 INR", 
  "2015-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR",
  "2016-09-01 10:00:01|info|Ac #3211001 AWT 5000 INR", 
  "2016-09-02 10:00:01|ERROR|Ac #3211001 AWT 10000 INR",
  "2016-10-01 10:00:01|error|Ac #3211001 AWT 8000 INR", 
  "2016-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR",
  "2016-12-01 10:00:01|Error|Ac #8211001 AWT 80000 INR", 
  "2016-12-02 10:00:01|error|Ac #9211001 AWT 90000 INR",
  "2016-12-10 10:00:01|error|Ac #3811001 AWT 15000 INR", 
  "2016-12-01 10:00:01|info|Ac #3219001 AWT 16000 INR"
)
val lastYearsLogsRDD = sc.parallelize(lastYearsLogs)
val logsWithErrors = lastYearsLogsRDD
  .map(log => log.toLowerCase())
  .filter(log => log.contains("error"))
  .persist(StorageLevel.MEMORY_ONLY_SER)

logsWithErrors.take(2)


// COMMAND ----------

logsWithErrors.count()   // faster


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.8: Understanding cluster topology Example-1 **

// COMMAND ----------

val people = List(
  "Sidharth,32",
  "Atul,33",
  "Sachin,30"
)
val peopleRdd = sc.parallelize(people)

def printPerson(line: Array[String]): Unit = {
  println("Person(" + line(0) + "," + line(1) + ")")
}

peopleRdd
  .map(line => line.split(","))
  .foreach(printPerson)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.9: Understanding cluster topology Example-2 **

// COMMAND ----------

peopleRdd
  .map(line => line.split(","))
  .take(2)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 1.10: mapPartitions() example **

// COMMAND ----------

def splitStr(recIter: Iterator[String]): Iterator[Array[String]] = {
  var resList: List[Array[String]] = List()
  for(rec <- recIter) {
    resList = resList :+ rec.split('|')
  }
  return resList.iterator
}

def arrToDouble(recIter: Iterator[Array[String]]): Iterator[Double] = {
  var resList: List[Double] = List()
  for(rec <- recIter) {
    resList = resList :+ rec(2).toDouble
  }
  return resList.iterator
}

sc.textFile("/FileStore/tables/txn_fct.csv")
  .filter(rec => !rec.contains("amount"))
  .mapPartitions(splitStr)
  .mapPartitions(arrToDouble)
  .reduce((n1, n2) => n1 + n2)

// COMMAND ----------

sc.textFile("/FileStore/tables/txn_fct.csv")
  .filter(rec => !rec.contains("amount"))
  .mapPartitions{recIter => recIter.map(rec => rec.split('|'))}
  .mapPartitions{recIter => recIter.map(rec => rec(2).toDouble)}
  .reduce((n1, n2) => n1 + n2)

// COMMAND ----------


