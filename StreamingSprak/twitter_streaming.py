from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from tweepy import OAuthHandler, AppAuthHandler, API

# Khởi tạo một SparkContext
sc = SparkContext("local", "TwitterStreamingAppNhom7")

# Khởi tạo một StreamingContext với batch interval 1 giây
ssc = StreamingContext(sc, 1)

# Sử dụng thông tin xác thực từ tweepy
consumer_key = "HCYfT8RdvYF44lNPgrXX7fcvf"
consumer_secret = "bS7Ts4GplPdDBiCwZ4Z77bguNL81SqcNM4lELcLyYJ072L3YN2"
# bearer_token = "AAAAAAAAAAAAAAAAAAAAAPgCpwEAAAAAYdmubUvbhTSLh7KX%2FJB%2B%2BVrsQX0%3DKAOlhmOHUgwUlHdbGqlrFFqnlDxjoXxQ7VwOxmte6QxFIqkYey"

# Khởi tạo API với xác thực đã tạo
auth = AppAuthHandler(consumer_key, consumer_secret)
api = API(auth)

# Tạo một luồng dữ liệu Spark Streaming từ Twitter
twitter_stream = ssc.socketTextStream("localhost", 9009)

# Xử lý dữ liệu từ luồng dữ liệu Twitter ở đây
# Ví dụ: In ra màn hình các tweet
twitter_stream.foreachRDD(lambda rdd: rdd.foreach(print))

# Khởi chạy Spark Streaming
ssc.start()

# Chờ đợi cho đến khi chấm dứt (Ctrl+C để dừng ứng dụng)
ssc.awaitTermination()
