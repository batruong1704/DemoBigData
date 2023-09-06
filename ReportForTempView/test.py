from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TemporaryViewExample").getOrCreate()
url = 'C:\PySpark\Project\ReportForTempView\data.csv'
data = spark.read.format('csv').option('header', 'true').load(url)

data.createOrReplaceTempView("user")

spark.sql("SELECT * FROM user").show()

