// Databricks notebook source
// MAGIC %md
// MAGIC ** Example 2.1: Pair RDDs - groupByKey **

// COMMAND ----------

val txnRdd = sc.textFile("/FileStore/tables/cred_txn.csv", 4)
txnRdd.take(5)

// COMMAND ----------

val accAndTxnCatRdd = txnRdd
  .filter(rec => !rec.contains("AccNum"))
  .map(rec => rec.split("~"))
  .map(rec => (rec(0), rec(3)))
  .groupByKey()

accAndTxnCatRdd.take(5)


// COMMAND ----------

accAndTxnCatRdd
  .filter(rec => rec._2.toArray.contains("Movies"))
  .take(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.2: Pair RDDs - reduceByKey **

// COMMAND ----------

val txnRdd = sc.textFile("/FileStore/tables/cred_txn.csv", 4)

val accAndTxnAmtRdd = txnRdd
  .filter(rec => !rec.contains("AccNum"))
  .map(rec => rec.split("~"))
  .map(rec => (rec(0), rec(1).toDouble))

accAndTxnAmtRdd
  .reduceByKey((amt1, amt2) => amt1 + amt2)
  .take(5)


// COMMAND ----------

reduceByKey() example: 
------------------------------------------------
RDD[(String, Int)]
part1		part2		part3
(c1,100)	(c2,200)	(c3,100)
(c2,200)	(c3,100)	(c1,200)
(c1,200)	(c2,100)	(c3,200)

.reduceByKey((n1, n2) => n1 + n2)
Step 1: (loal aggregates)
part1		part2		part3
(c1,300)	(c2,400)	(c3,300)
(c2,200)	(c3,100)	(c1,200)

Step 2: Shuffleing of intermediate key and value pairs
part1		part2		part3
(c1,300)	(c2,400)	(c3,300)
(c1,200)	(c2,200)	(c3,100)

Step 3: FInal consolidation happens
part1		part2		part3
(c1,500)	(c2,600)	(c3,400) 

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.3: Pair RDDs - aggregateByKey **

// COMMAND ----------

val txnRdd = sc.textFile("/FileStore/tables/cred_txn.csv", 4)

val accCatAndTxnAmtRdd = txnRdd
  .filter(rec => !rec.contains("AccNum"))
  .map(rec => rec.split("~"))
  .map(rec => (rec(0), (rec(3), rec(1).toDouble)))


// COMMAND ----------

accCatAndTxnAmtRdd
  .reduceByKey((catAmt1, catAmt2) => if(catAmt1._2 > catAmt2._2) catAmt1._2 else catAmt2._2)
  .take(5)


// COMMAND ----------

accCatAndTxnAmtRdd
  .reduceByKey((catAmt1, catAmt2) => if(catAmt1._2 > catAmt2._2) catAmt1 else catAmt2)
  .map(rec => (rec._1, rec._2._2))
  .take(5)


// COMMAND ----------

// Sequence operation : Finding Maximum Amount from a single partition
def sumAtPartition(acc: Double, element: (String, Double)): Double = {
  if(acc > element._2) 
    acc 
  else 
    element._2
}

// Combiner Operation : Finding Maximum Amount out Partition-Wise Accumulators
def sumAcrossPartition(acc1: Double, acc2: Double): Double = {
  if(acc1 > acc2) 
    acc1 
  else 
    acc2
}

accCatAndTxnAmtRdd
  .aggregateByKey(Double.MaxValue)(sumAtPartition, sumAcrossPartition)
  .collect()


// COMMAND ----------

aggregateByKey() example:
------------------------------------------------
RDD[(String, Int)]
part1			part2			part3
(c1,(cat1,100))	(c2,(cat2,200))	(c3,(cat1,100))
(c2,(cat2,200))	(c3,(cat3,100))	(c1,(cat2,200))
(c1,(cat1,200))	(c2,(cat2,100))	(c3,(cat3,200))

.aggregateByKey[-Inf]((tc1, tc2) => t, (t1, t2) => t)
Step 1: (loal aggregates)
part1		part2		part3
(c1,200)	(c2,200)	(c3,200)
(c2,200)	(c3,100)	(c1,200)

Step 2: Shuffleing of intermediate key and value pairs
part1		part2		part3
(c1,200)	(c2,200)	(c3,200)
(c1,200)	(c2,200)	(c3,100)

Step 3: FInal consolidation happens
part1		part2		part3
(c1,200)	(c2,200)	(c3,200)

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.4: Pair RDDs - aggregateByKey (continued..) **

// COMMAND ----------

val studentRdd = sc.parallelize(
  Array(
    ("Ravi", "Maths", 83), 
    ("Ravi", "Physics", 74), 
    ("Ravi", "Chemistry", 91), 
    ("Ravi", "Biology", 82), 
    ("Kiran", "Maths", 69), 
    ("Kiran", "Physics", 62), 
    ("Kiran", "Chemistry", 97), 
    ("Kiran", "Biology", 80), 
    ("Rajesh", "Maths", 78), 
    ("Rajesh", "Physics", 73), 
    ("Rajesh", "Chemistry", 68), 
    ("Rajesh", "Biology", 87), 
    ("Raghu", "Maths", 87), 
    ("Raghu", "Physics", 93), 
    ("Raghu", "Chemistry", 91), 
    ("Raghu", "Biology", 74), 
    ("Dharan", "Maths", 56), 
    ("Dharan", "Physics", 65), 
    ("Dharan", "Chemistry", 71), 
    ("Dharan", "Biology", 68), 
    ("Jyoti", "Maths", 86), 
    ("Jyoti", "Physics", 62), 
    ("Jyoti", "Chemistry", 75), 
    ("Jyoti", "Biology", 83), 
    ("Ankita", "Maths", 63), 
    ("Ankita", "Physics", 69), 
    ("Ankita", "Chemistry", 64), 
    ("Ankita", "Biology", 60)), 
  3)
 

// COMMAND ----------

// Sequence operation : Finding Maximum Marks from a single partition
def seqOperation(acc: Int, element: (String, Int)): Int = { 
  if(acc > element._2) 
    acc 
  else 
    element._2
}

// Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def combOperation(acc1: Int, acc2: Int): Int = { 
  if(acc1 > acc2) 
    acc1 
  else 
    acc2
}

// Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
val initVal = 0
studentRdd
  .map(student => (student._1, (student._2, student._3)))
  .aggregateByKey(initVal)(seqOperation, combOperation) 
  .collect
 

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.5: Pair RDDs - mapValues **

// COMMAND ----------

val txnRdd = sc.textFile("/FileStore/tables/cred_txn.csv", 5)

val accAndTxnAmtRdd = txnRdd
  .filter(rec => !rec.contains("AccNum"))
  .map(rec => rec.split("~"))
  .map(rec => (rec(0), rec(1).toDouble))

accAndTxnAmtRdd
  .mapValues(amt => (amt, 1))
  .reduceByKey((amt1, amt2) => (amt1._1 + amt2._1, amt1._2 + amt2._2))
  .mapValues(rec => rec._1/rec._2)
  .take(5)


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.6: Pair RDDs - countByKey **

// COMMAND ----------

val txnRdd = sc.textFile("/FileStore/tables/cred_txn.csv", 5)

txnRdd
  .filter(rec => !rec.contains("AccNum"))
  .map(rec => rec.split("~"))
  .map(rec => (rec(0), rec(1).toDouble))
  .countByKey


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.7: Joins - inner join **

// COMMAND ----------

val txnRdd = sc.textFile("/FileStore/tables/cred_txn.csv", 5)
  .filter(rec => !rec.contains("AccNum"))
  .map(rec => rec.split("~"))
  .map(rec => (rec(0), rec(1).toDouble))

txnRdd.take(5)


// COMMAND ----------

val rawCustRdd = sc.textFile("/FileStore/tables/cust_name.csv", 1)
rawCustRdd.take(5)


// COMMAND ----------

val custRdd = rawCustRdd
  .filter(rec => !rec.contains("AccNum"))
  .map(rec => rec.split("~"))
  .map(rec => (rec(0), rec(1)))

custRdd.take(5)


// COMMAND ----------

val trackedCustomers = txnRdd.join(custRdd)
trackedCustomers.collect()

// COMMAND ----------

trackedCustomers.map(rec => rec._1).distinct.collect

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.8: Joins - outer join **

// COMMAND ----------

// Left outer join
val trackedCustomers = txnRdd.leftOuterJoin(custRdd)
trackedCustomers.map(rec => rec._1).distinct.collect


// COMMAND ----------

val trackedCustomers = txnRdd.fullOuterJoin(custRdd)
trackedCustomers.map(rec => rec._1).distinct.collect()


// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.9: Accumulator **

// COMMAND ----------

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


// COMMAND ----------

var errorLogLines: Int = 0
lastYearsLogsRDD.foreach { log =>
  if(log.toUpperCase().contains("ERROR"))
    errorLogLines = errorLogLines + 1
}
println(errorLogLines)

// COMMAND ----------

var errorLogLines = sc.longAccumulator("Errors")
lastYearsLogsRDD.foreach { log =>
  if(log.toUpperCase().contains("ERROR"))
    errorLogLines.add(1)
}
errorLogLines.value

// COMMAND ----------

// MAGIC %md
// MAGIC ** Example 2.10: Broadcast Variable **

// COMMAND ----------

val input = sc.parallelize(List(1, 2, 3))
val broadcastVar = sc.broadcast(2)
 
val added = input.map(x => broadcastVar.value + x)
 
added.collect()

// COMMAND ----------


