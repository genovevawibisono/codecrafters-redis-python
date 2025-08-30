import socket  # noqa: F401
import threading

def handle(connection):
    while True:
        data = connection.recv(1024)
        if data == None:
            break
        connection.sendall(b"+PONG\r\n")
    connection.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept() # wait for client
        client_thread = threading.Thread(target=handle, args=(connection,))
        client_thread.start()

if __name__ == "__main__":
    main()
