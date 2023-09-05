from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("TemporaryViewExample").getOrCreate()

# Tạo một DataFrame từ danh sách dữ liệu
data = spark.read.format('csv').option('header', 'true').load('C:\\Users\\nguye\\OneDrive - dungnguyentstb\\Documents\\Tài liệu học tập\\BigData\\DemoBigData\ReportForTempView\\data.csv')
# Tạo một Temporary View từ DataFrame
data.createOrReplaceTempView("temp_view")

# Tạo một Global Temporary View từ DataFrame
data.createOrReplaceGlobalTempView("global_temp_view")

# Truy vấn Temporary View, Global Temporary View  từ phiên làm việc gốc
spark.sql("SELECT * FROM temp_view").show()
spark.sql("SELECT * FROM global_temp.global_temp_view").show()


# Khởi tạo một phiên làm việc mới
spark2 = spark.newSession()

# Truy vấn Global Temporary View từ phiên làm việc mới, còn TemporaryView thì đéo được đâu :))
spark2.sql("SELECT * FROM global_temp.global_temp_view").show()
spark2.sql("SELECT * FROM temp_view").show()
