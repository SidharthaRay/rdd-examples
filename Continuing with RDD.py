# Databricks notebook source
# MAGIC %md
# MAGIC ** Example 2.1: Pair RDDs - groupByKey **

# COMMAND ----------

txn_rdd = sc.textFile("/FileStore/tables/cred_txn.csv", 5) \
  .mapPartitionsWithIndex(lambda index, partition: iter(list(partition)[1:]) if index == 0 else partition) \
  .map(lambda rec: rec.split('~')) \
  .map(lambda rec: (rec[0], rec[3])) 

txn_rdd.take(5)

# COMMAND ----------

acc_txncat_rdd = txn_rdd \
  .groupByKey() \
  .map(lambda rec: (rec[0], list(rec[1]))) 

acc_txncat_rdd.take(5)


# COMMAND ----------

acc_txncat_rdd \
  .map(lambda rec: (rec[0], set(rec[1]))) \
  .take(5)

# COMMAND ----------

acc_txncat_rdd \
  .filter(lambda rec: 'Movies' in rec[1]) \
  .take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.2: Pair RDDs - reduceByKey **

# COMMAND ----------

txn_rdd = sc.textFile('/FileStore/tables/cred_txn.csv', 5)

acc_txncat_rdd = txn_rdd \
  .mapPartitionsWithIndex(lambda index, partition: iter(list(partition)[1:]) if index == 0 else partition) \
  .map(lambda rec: rec.split('~')) \
  .map(lambda rec: (rec[0], float(rec[1])))

acc_txncat_rdd \
  .reduceByKey(lambda amt1, amt2: amt1 + amt2) \
  .take(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.3: Pair RDDs - aggregateByKey **

# COMMAND ----------

txn_rdd = sc.textFile('/FileStore/tables/cred_txn.csv', 5)

acc_txncat_rdd = txn_rdd \
  .mapPartitionsWithIndex(lambda index, partition: iter(list(partition)[1:]) if index == 0 else partition) \
  .map(lambda rec: rec.split('~')) \
  .map(lambda rec: (rec[0], (rec[3], float(rec[1]))))


# COMMAND ----------

acc_txncat_rdd \
  .reduceByKey(lambda catAmt1, catAmt2: (catAmt1[1] if(catAmt1[1] > catAmt2[1]) else catAmt2[1])) \
  .take(5)


# COMMAND ----------

acc_txncat_rdd \
  .reduceByKey(lambda catAmt1, catAmt2: catAmt1 if(catAmt1[1] > catAmt2[1]) else catAmt2) \
  .map(lambda rec: (rec[0], rec[1][1])) \
  .take(5)


# COMMAND ----------

import sys

# Sequence operation : Finding Maximum Amount from a single partition
def sumAtPartition(acc, element):
  if(acc > element[1]): 
    return acc 
  else: 
    return element[1]

# Combiner Operation : Finding Maximum Amount out Partition-Wise Accumulators
def sumAcrossPartition(acc1, acc2):
  if(acc1 > acc2): 
    return acc1 
  else: 
    return acc2

acc_txncat_rdd \
  .aggregateByKey(sys.float_info.min, sumAtPartition, sumAcrossPartition) \
  .collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.4: Pair RDDs - aggregateByKey (continued..) **

# COMMAND ----------

student_rdd = sc.parallelize( \
  [
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
    ("Ankita", "Biology", 60)],  
  3)
 

# COMMAND ----------

# Sequence operation : Finding Maximum Marks from a single partition
def seqOperation(acc, element): 
  if(acc > element[1]): 
    return acc 
  else: 
    return element[1]

# Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def combOperation(acc1, acc2): 
  if(acc1 > acc2): 
    return acc1 
  else: 
    return acc2

# Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
student_rdd \
  .map(lambda student: (student[0], (student[1], student[2]))) \
  .aggregateByKey(0, seqOperation, combOperation) \
  .collect()
 

# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.5: Pair RDDs - mapValues **

# COMMAND ----------

txn_rdd = sc.textFile('/FileStore/tables/cred_txn.csv', 5)

acc_txnamt_rdd = txn_rdd \
  .mapPartitionsWithIndex(lambda index, partition: iter(list(partition)[1:]) if index == 0 else partition) \
  .map(lambda rec: rec.split('~')) \
  .map(lambda rec: (rec[0], float(rec[1])))

acc_txnamt_rdd \
  .mapValues(lambda amt: (amt, 1)) \
  .reduceByKey(lambda amt1, amt2: (amt1[0] + amt2[0], amt1[1] + amt2[1])) \
  .mapValues(lambda rec: rec[0]/rec[1]) \
  .take(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.6: Pair RDDs - countByKey **

# COMMAND ----------

txn_rdd = sc.textFile('/FileStore/tables/cred_txn.csv', 5)

txn_rdd \
  .mapPartitionsWithIndex(lambda index, partition: iter(list(partition)[1:]) if index == 0 else partition) \
  .map(lambda rec: rec.split('~')) \
  .map(lambda rec: (rec[0], float(rec[1]))) \
  .countByKey()


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.7: Joins - inner join **

# COMMAND ----------

txn_rdd = sc.textFile('/FileStore/tables/cred_txn.csv', 5) \
  .mapPartitionsWithIndex(lambda index, partition: iter(list(partition)[1:]) if index == 0 else partition) \
  .map(lambda rec: rec.split('~')) \
  .map(lambda rec: (rec[0], float(rec[1]))) \

txn_rdd.take(5)

# COMMAND ----------

raw_cust_rdd = sc.textFile('/FileStore/tables/cust_name.csv', 1)
raw_cust_rdd.take(5)


# COMMAND ----------

cust_rdd = raw_cust_rdd \
  .mapPartitionsWithIndex(lambda index, partition: iter(list(partition)[1:]) if index == 0 else partition) \
  .map(lambda rec: rec.split('~')) \
  .map(lambda rec: (rec[0], rec[1]))

cust_rdd.take(5)


# COMMAND ----------

tracked_customers = txn_rdd.join(cust_rdd)
tracked_customers.take(5)

# COMMAND ----------

tracked_customers.map(lambda rec: rec[0]).distinct().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.8: Joins - outer join **

# COMMAND ----------

# Left outer join
tracked_customers = txn_rdd.leftOuterJoin(cust_rdd)
tracked_customers.map(lambda rec: rec[0]).distinct().collect()


# COMMAND ----------

tracked_customers = txn_rdd.fullOuterJoin(cust_rdd)
tracked_customers.map(lambda rec: rec[0]).distinct().collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.9: Accumulator **

# COMMAND ----------

last_years_logs = [
  '2015-09-01 10:00:01|Error|Ac #3211001 ATW 10000 INR', 
  '2015-09-02 10:00:07|Info|Ac #3281001 ATW 11000 INR',
  '2015-10-01 10:00:09|error|Ac #3311001 AWT 10500 INR', 
  '2015-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR',
  '2016-09-01 10:00:01|info|Ac #3211001 AWT 5000 INR', 
  '2016-09-02 10:00:01|ERROR|Ac #3211001 AWT 10000 INR',
  '2016-10-01 10:00:01|error|Ac #3211001 AWT 8000 INR', 
  '2016-11-01 10:00:01|error|Ac #3211001 AWT 10000 INR',
  '2016-12-01 10:00:01|Error|Ac #8211001 AWT 80000 INR', 
  '2016-12-02 10:00:01|error|Ac #9211001 AWT 90000 INR',
  '2016-12-10 10:00:01|error|Ac #3811001 AWT 15000 INR', 
  '2016-12-01 10:00:01|info|Ac #3219001 AWT 16000 INR'
]
last_years_logs_rdd = sc.parallelize(last_years_logs)


# COMMAND ----------

error_log_lines = 0
last_years_logs_rdd.foreach(lambda log: ((error_log_lines + 1) if('ERROR' in log.upper()) else error_log_lines))
print(error_log_lines)


# COMMAND ----------

error_log_lines = sc.accumulator(0)
last_years_logs_rdd.foreach(lambda log: (error_log_lines.add(1) if('ERROR' in log.upper()) else error_log_lines.add(0)))
error_log_lines.value

# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 2.10: Broadcast Variable **

# COMMAND ----------

broadcast_var = sc.broadcast(2)
 
input_rdd = sc.parallelize([1, 2, 3])
added_rdd = input_rdd.map(lambda num: broadcast_var.value + num)
 
added_rdd.collect()

# COMMAND ----------


