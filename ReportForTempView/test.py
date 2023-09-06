from pyspark.sql import SparkSession

# Khởi tạo một phiên làm việc Spark khác
spark2 = SparkSession.builder.appName("new_session").getOrCreate()

# Truy vấn Global Temporary View từ phiên làm việc trước
result = spark2.sql("SELECT * FROM global_temp.global_temp_view")

# Hiển thị kết quả
result.show()

# Kết thúc phiên làm việc Spark
spark2.stop()
