from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("TemporaryViewExample").getOrCreate()

# Tạo một DataFrame từ danh sách dữ liệu
data = spark.read.format('csv').option('header', 'true').load('C:/LearnBigData/BaTruong/ReportForTempView/data.csv')
# Tạo một temporary view từ DataFrame
view_name = "people_temp_view"
data.createTempView(view_name)

# Truy vấn dữ liệu từ temporary view bằng SQL
sql_query = "SELECT * FROM " + view_name
result_df = spark.sql(sql_query)

# Hiển thị kết quả
result_df.show()
