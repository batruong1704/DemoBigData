import socket

# Khởi tạo socket server
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(("localhost", 8888))
server_socket.listen(1)
print("Listening on port 8888...")

# Chấp nhận kết nối từ client
client_socket, client_address = server_socket.accept()
print("Accepted connection from:", client_address)

# Gửi dữ liệu đến client
while True:
    message = input("Nhập dữ liệu: ")
    client_socket.send(message.encode())
