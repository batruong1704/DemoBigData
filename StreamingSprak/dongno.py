from pyspark.sql import SparkSession

logFile = "C:\Program Files\spark-3.4.1-bin-hadoop3/README.md"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("\n \n =>  Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()