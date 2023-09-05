import tweepy
import socket

access_token = "1698348641642266625-HXMQ398Xxi1a3jyDiKlNuxU4i6Eoa6"
access_token_secret = "OdX5tE5rc9K4rWwGdctyIx4Ma940IDjxYei8LGdVDJNDy"
consumer_key = "HCYfT8RdvYF44lNPgrXX7fcvf"
consumer_secret = "bS7Ts4GplPdDBiCwZ4Z77bguNL81SqcNM4lELcLyYJ072L3YN2"
client_secret = "kOz2yQJeNMTrs5RScyd9Tk3SJ35TuEjhe9Hdz9R_muzIeKu3h4"

# Tạo một phiên xác thực OAuth 1
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Khởi tạo API với xác thực đã tạo
api = tweepy.API(auth, wait_on_rate_limit=True)

# Kiểm tra xem kết nối đã thành công hay chưa
if not api:
    print("Không thể kết nối đến Twitter API. Vui lòng kiểm tra thông tin xác thực.")
else:
    print("Kết nối thành công đến Twitter API.")

# Tạo kết nối socket đến Spark Streaming
TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")

# Lấy tweet từ Twitter và gửi chúng qua kết nối socket đến Spark Streaming
twitter_handle = "Elon Musk"  # Thay thế bằng tên tài khoản Twitter bạn quan tâm

def get_and_send_tweets():
    tweets = api.user_timeline(screen_name=twitter_handle, count=10)
    for tweet in tweets:
        conn.send(tweet.text.encode("utf-8") + b'\n')

get_and_send_tweets()
