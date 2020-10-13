# Databricks notebook source
# MAGIC %md
# MAGIC ** Example 1.1 - Word count example **

# COMMAND ----------

text_rdd = sc.textFile('/FileStore/tables/SampleText.txt') # RDD[str]

def str_split(line):
  return line.split(' ')
splitted_rdd = text_rdd.flatMap(str_split) # RDD[List[str]]

def word_pair(word):
  return (word, 1)
word_pair_rdd = splitted_rdd.map(word_pair)  # RDD[(str, int)]

def sum(cnt1, cnt2):
  return (cnt1 + cnt2)
word_count_rdd = word_pair_rdd.reduceByKey(sum) # RDD[(str, int)]

word_count_rdd.take(5) # dict(str, int)


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.2 - Creating RDD with parallelize() function **

# COMMAND ----------

num_rdd = sc.parallelize(range(1, 11))  # RDD of int
num_rdd.getNumPartitions() # int

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

num_rdd_parts = num_rdd.glom()
num_rdd_parts.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.3 - Applying some basic data transformation **

# COMMAND ----------

txn_list = ['txn01~1000', 
            'txn02~2000', 
            'txn03~1000', 
            'txn04~2000']
txn_rdd = sc.parallelize(txn_list)

def strSplit(line):
  return line.split('~')
splitted_txn_rdd = txn_rdd.map(strSplit)

def getAmount(rec):
  return float(rec[1])
txn_amt_rdd = splitted_txn_rdd.map(getAmount)

def sum(amt1, amt2):
  return (amt1 + amt2)
txn_amt_rdd.reduce(sum)


# COMMAND ----------

txn_list = ['txn01~1000', 'txn02~2000', 'txn03~1000', 'txn04~2000']
sc.parallelize(txn_list)\
  .map(lambda rec: rec.split('~'))\
  .map(lambda rec: float(rec[1]))\
  .reduce(lambda amt1, amt2: amt1 + amt2)


# COMMAND ----------

server_logs = [
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

logs_rdd = sc.parallelize(server_logs)

logs_rdd.filter(lambda log: '2016-12' in log).count()


# COMMAND ----------

logs_rdd \
  .map(lambda log: log.lower()) \
  .filter(lambda log: '2016-12' in log and 'error' in log) \
  .count()


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.4 - repartition() and coalesce() function **

# COMMAND ----------

bigger_rdd = sc.parallelize(range(1, 1001), 8)
bigger_rdd.take(3)

# COMMAND ----------

bigger_rdd.getNumPartitions()

# COMMAND ----------

bigger_rdd.count()

# COMMAND ----------

bigger_rdd\
  .glom()\
  .map(lambda rec: len(rec))\
  .collect()

# COMMAND ----------

bigger_rdd.repartition(10).saveAsTextFile('/FileStore/tables/biggerRdd/ten')
sc.textFile('/FileStore/tables/biggerRdd/ten').getNumPartitions()


# COMMAND ----------

bigger_rdd.repartition(10).glom().map(lambda rec: len(rec)).collect()

# COMMAND ----------

bigger_rdd.repartition(5).saveAsTextFile('/FileStore/tables/biggerRdd/four')
sc.textFile('/FileStore/tables/biggerRdd/four').getNumPartitions()


# COMMAND ----------

bigger_rdd.repartition(5).glom().map(lambda rec: len(rec)).collect()

# COMMAND ----------

bigger_rdd.coalesce(6).saveAsTextFile('dbfs:/FileStore/tables/biggerRdd/coalesce')
sc.textFile('/FileStore/tables/biggerRdd/coalesce').getNumPartitions()

# Homework: Is the number of records same in each of the partitions for both repartition() and coalesce()? Justify the full shufling meaning!


# COMMAND ----------

bigger_rdd\
  .coalesce(6)\
  .glom()\
  .map(lambda rec: len(rec))\
  .collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.5 - Saving data as TextFile, SequenceFile and NewAPIHadoopFile **

# COMMAND ----------

reg_rdd = sc.textFile('/FileStore/tables/KC_Extract_1_20171009.csv') \
  .map(lambda rec: rec.split('|', -1))

reg_rdd.take(3)


# COMMAND ----------

reg_rdd.saveAsTextFile('/FileStore/tables/Reg_v12/')
sc.textFile('dbfs:/FileStore/tables/Reg_v12/').take(3)


# COMMAND ----------

reg_rdd.map(lambda rec: (rec[4], rec[10])).saveAsSequenceFile('dbfs:/FileStore/tables/Reg_v43')
sc.sequenceFile('dbfs:/FileStore/tables/Reg_v43').take(5)


# COMMAND ----------

reg_rdd.map(lambda record: (record[2], record[10])) \
.saveAsNewAPIHadoopFile("dbfs:/FileStore/tables/Reg_v53/", \
                         'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat', \
                         'org.apache.hadoop.io.Text', \
                         'org.apache.hadoop.io.Text'
                         )


# COMMAND ----------

sc.newAPIHadoopFile("/FileStore/tables/Reg_v53",
                   'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat',
                   'org.apache.hadoop.io.Text', \
                   'org.apache.hadoop.io.Text') \
  .take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.6: Set Operations **

# COMMAND ----------

et_log = [
  "2017-10-13 10:00:00|RIL|830.00",
  "2017-10-13 10:00:10|MARUTI SUZUKI|8910.00",
  "2017-10-13 10:00:20|RIL|835.00",
  "2017-10-13 10:00:30|MARUTI SUZUKI|8890.00"
]
et_log_rdd = sc.parallelize(et_log)

mc_log = [
  "2017-10-13 10:00:20|RIL|835.00",
  "2017-10-13 10:00:40|MARUTI SUZUKI|8870.00"
]
mc_log_rdd = sc.parallelize(mc_log)


# COMMAND ----------

# Union - Share price example
print('Total number of stocks: {0}'.format(et_log_rdd.union(mc_log_rdd).distinct().count()))
et_log_rdd.union(mc_log_rdd).distinct().collect()


# COMMAND ----------

# Intersection
et_log_rdd.intersection(mc_log_rdd).collect()


# COMMAND ----------

# Set difference
et_log_rdd.subtract(mc_log_rdd).collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.7: Other Useful RDD Actions **

# COMMAND ----------

num_rdd = sc.parallelize(range(1, 11))
num_rdd.takeSample(False, 5)

# COMMAND ----------

num_rdd.takeSample(True, 5)


# COMMAND ----------

num_rdd.takeSample(False, 5, 100)


# COMMAND ----------

sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7]).takeOrdered(6)


# COMMAND ----------

sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7]).takeOrdered(6, lambda x: -x)


# COMMAND ----------

sc.parallelize([(10, 1), (2, 9), (3, 4), (5, 6), (2, 7)]).takeOrdered(6, lambda x: -x[0])


# COMMAND ----------

sc.parallelize([(10, 1), (2, 9), (3, 4), (5, 6), (2, 7)]).takeOrdered(6, lambda x: (-x[0], -x[1]))


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.8: Caching and Persistence **

# COMMAND ----------

from pyspark import StorageLevel
server_logs = [
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
]
server_logs_rdd = sc.parallelize(server_logs)
logs_with_errors = server_logs_rdd \
  .map(lambda log: log.lower()) \
  .filter(lambda log: 'error' in log) \
  .persist(StorageLevel.MEMORY_ONLY_SER) 

logs_with_errors.take(2)


# COMMAND ----------

logs_with_errors.count()   # faster


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.9: Understanding cluster topology Example **

# COMMAND ----------

people = [
  "Sidharth,32",
  "Atul,33",
  "Sachin,30"
]
people_rdd = sc.parallelize(people) \
  .map(lambda line: line.split(',')) 


# COMMAND ----------

people_rdd \
  .foreach(lambda rec: print('{0}, {1}'.format(rec[0], rec[1])))


# COMMAND ----------

people_rdd.take(2)


# COMMAND ----------

# MAGIC %md
# MAGIC ** Example 1.10: mapPartitions() example **

# COMMAND ----------

def strSplit(partition):
  sec_iterator = []
  for rec in partition:
    sec_iterator.append(rec.split('|'))
  return sec_iterator
      
def getAmount(partition):
  sec_iterator = []
  for rec in partition:
    sec_iterator.append(float(rec[2]))
  return sec_iterator

sc.textFile('/FileStore/tables/txn_fct.csv') \
  .filter(lambda rec: 'amount' not in rec) \
  .mapPartitions(strSplit) \
  .mapPartitions(getAmount) \
  .reduce(lambda amt1, amt2: amt1 + amt2)


# COMMAND ----------

sc.textFile('/FileStore/tables/txn_fct.csv') \
  .filter(lambda rec: 'amount' not in rec) \
  .mapPartitions(lambda partition: (rec.split('|') for rec in partition)) \
  .mapPartitions(lambda partition: (float(rec[2]) for rec in partition)) \
  .reduce(lambda amt1, amt2: amt1 + amt2)


# COMMAND ----------

sc.textFile('/FileStore/tables/txn_fct.csv') \
  .mapPartitionsWithIndex(lambda index, partition: iter(list(partition)[1:]) if index == 0 else partition) \
  .take(5)

# COMMAND ----------


