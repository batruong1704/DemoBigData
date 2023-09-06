# Hướng dẫn
**Bước 1: Khởi tạo một phiên làm việc Spark**
```python
# Import SparkSession
from pyspark.sql import SparkSession

# Khởi tạo một phiên làm việc Spark
spark = SparkSession.builder.appName("my_app").getOrCreate()
```

**Bước 2: Tạo DataFrame**
- Tự tạo data:
```python
data = [("Alice", 25), ("Bob", 30), ("Carol", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
```
- Lấy sẵn file csv:
```python
df = spark.read.format('csv').option('header', 'true').load('C:\data.csv')
```
**Bước 3: Tạo Temporary View**

```python
df.createOrReplaceTempView("people")
```
- Tạo global view
```python
df.createOrReplaceGlobalTempView("people")
```
**Bước 4: Thực hiện truy vấn SQL trên Temporary View**
```python
result = spark.sql("SELECT * FROM people WHERE Age >= 30")
```
- Đối với global view
```python
result = spark.sql("SELECT * FROM global_temp.people WHERE Age >= 30")
```
**Bước 5: Hiển thị kết quả**
```python
result.show()
```

**Bước 6: Kết thúc phiên làm việc Spark**
```python
spark.stop()
```

## A. Một vài lệnh khác cho temporary view:
**1. Xem danh sách các Temporary View hiện có trong phiên làm việc Spark hiện tại**

```python
# Lấy danh sách các Temporary View
tables = spark.catalog.listTables()
for table in tables:
    print(table.name)
```

**2. Xem thông tin chi tiết về một Temporary View cụ thể**

```python
# Lấy thông tin về Temporary View cụ thể
table_info = spark.catalog.getTable("temp_view")

# In ra schema của Temporary View
print(table_info.schema)
```

**3. Kiểm tra sự tồn tại của một Temporary View**

```python
# Kiểm tra sự tồn tại của Temporary View
if spark.catalog.tableExists("temp_view"):
    print("Temporary View 'temp_view' tồn tại.")
else:
    print("Temporary View 'temp_view' không tồn tại.")
```

**4. Đổi tên Temporary View**

```python
# Đổi tên Temporary View
spark.catalog.renameTempView("temp_view", "new_temp_view")
```

**5. Xóa Temporary View**

```python
# Xóa Temporary View
spark.catalog.dropTempView("new_temp_view")
```

## B. Một vài lệnh khác cho global view:

**Lệnh 1: Xem tất cả các Global Temporary View hiện có**

```python
# Lấy danh sách các Global Temporary View hiện có
global_temp_views = spark.catalog.listGlobalTempTables()
for view in global_temp_views:
    print(view.name)

# Lấy thông tin về Global Temporary View cụ thể
global_table_info = spark.catalog.getGlobalTempView("global_temp_view")

# In ra schema của Global Temporary View
print(global_table_info.schema)
```
**2. Xóa Global Temporary View**

```python
# Xóa Global Temporary View
spark.catalog.dropGlobalTempView("global_temp_view")
```

**3. Xóa tất cả Global Temporary Views**

```python
# Xóa tất cả Global Temporary Views
spark.catalog.clearGlobalTempView()
```

**4. Đổi tên Global Temporary View**

```python
# Đổi tên Global Temporary View
spark.catalog.renameGlobalTempView("global_temp_view", "new_global_temp_view")
```

**5. Thực hiện truy vấn kết hợp giữa Global Temporary View và DataFrame**

```python
# Tạo một DataFrame khác
data2 = spark.createDataFrame([(1, "A"), (2, "B")], ["ID", "Value"])

# Truy vấn kết hợp giữa Global Temporary View và DataFrame
result = spark.sql("SELECT * FROM global_temp.global_temp_view v INNER JOIN data2 d ON v.ID = d.ID")

# Hiển thị kết quả
result.show()
```
# C. Một vài hàm xử lý
❤️ Sau khi đã khởi tạo phiên làm việc + input data, tiếp theo ta sẽ thực hiện xử lý data:

```python
# Định nghĩa Window 
from pyspark.sql.window import Window
from pyspark.sql.functions import col

window_spec = Window.orderBy(col("timestamp_column"))
# Xử lý
from pyspark.sql.functions import sum

result = data.withColumn("sliding_sum", sum(col("value_column")).over(window_spec))
# Hiển thị kết quả
result.show()
```

🥵 Các hàm có thể sử dụng cho việc xử lý:

| Hàm                         | Mô Tả                                                                                   |
|-----------------------------|------------------------------------------------------------------------------------------|
| sum(col)                    | Tính tổng giá trị của một cột cụ thể trong cửa sổ.                                      |
| avg(col)                    | Tính giá trị trung bình của một cột cụ thể trong cửa sổ.                                |
| min(col)                    | Tìm giá trị nhỏ nhất của một cột cụ thể trong cửa sổ.                                   |
| max(col)                    | Tìm giá trị lớn nhất của một cột cụ thể trong cửa sổ.                                  |
| first(col)                  | Lấy giá trị đầu tiên của một cột cụ thể trong cửa sổ.                                  |
| last(col)                   | Lấy giá trị cuối cùng của một cột cụ thể trong cửa sổ.                                 |
| lead(col, n)                | Lấy giá trị của cột cụ thể n hàng sau trong cửa sổ.                                    |
| lag(col, n)                 | Lấy giá trị của cột cụ thể n hàng trước trong cửa sổ.                                  |
| row_number()                | Đánh số hàng trong cửa sổ từ 1 đến n, dựa trên sắp xếp đã định nghĩa.                    |
| rank()                      | Xếp hạng hàng trong cửa sổ dựa trên giá trị của cột cụ thể.                            |
| dense_rank()                | Xếp hạng hàng trong cửa sổ mà không có giá trị xếp hạng trùng lặp.                     |
| percent_rank()              | Tính xếp hạng hàng dưới dạng phần trăm, từ 0 đến 1.                                     |
| ntile(n)                    | Chia dữ liệu thành n phần bằng nhau và trả về số phần mà hàng hiện tại thuộc về.        |
| cume_dist()                 | Tính xác suất cột cụ thể có giá trị không lớn hơn giá trị hiện tại.                     |
| lead(col, n, default)      | Giống như lead và lag, nhưng có thể chỉ định giá trị mặc định nếu không có hàng thỏa mãn. |
| lag(col, n, default)       | Giống như lead và lag, nhưng có thể chỉ định giá trị mặc định nếu không có hàng thỏa mãn. |
| collect_list(col)           | Tạo danh sách các giá trị trong cửa sổ.                                                |
| collect_set(col)            | Tạo tập hợp các giá trị trong cửa sổ.                                                   |
| count(col)                  | Đếm số hàng trong cửa sổ.                                                              |
