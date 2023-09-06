from pyspark.sql import SparkSession

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("TemporaryViewExample").getOrCreate()

# Tạo một DataFrame từ danh sách dữ liệu
data = spark.read.format('csv').option('header', 'true').load('C:/PySpark/Project/ReportForTempView/data.csv')
# Tạo một Temporary View từ DataFrame
data.createOrReplaceTempView("temp_view")

# Tạo một Global Temporary View từ DataFrame
data.createOrReplaceGlobalTempView("my_global_view")

# Truy vấn Temporary View, Global Temporary View  từ phiên làm việc gốc
print("--------------------------Phiên gốc-----------------------------------")
print("TemporaryView:")
spark.sql("SELECT * FROM temp_view").show()
print("GlobalTempView:")
spark.sql("SELECT * FROM global_temp.my_global_view").show()


# Khởi tạo một phiên làm việc mới
spark2 = spark.newSession()
print("-----------------------------Phiên thứ 2--------------------------------")

print("GlobalTempView:")
spark2.sql("SELECT * FROM global_temp.my_global_view").show()
# print("TemporaryView:")
# spark2.sql("SELECT * FROM temp_view").show()


# Lấy thông tin về các thuộc tính của bảng
table_description = data.describe()

# In ra thông tin thuộc tính
table_description.show()

# Lấy thông tin về bảng
table_info = spark.catalog.getTable("global_temp.my_global_view")

# In ra thông tin về bảng
print("Tên bảng: ", table_info.name)
print("Cơ sở dữ liệu: ", table_info.database)
print("Mô tả: ", table_info.description)
print("Loại bảng: ", table_info.tableType)
print("Mô tả chi tiết: ", table_info.schema.simpleString())
