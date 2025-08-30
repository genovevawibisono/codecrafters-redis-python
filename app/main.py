import socket  # noqa: F401
import threading

class Handler:
    def __init__(self):
        self.dictionary = dict()

    def handle(self, connection):
        while True:
            data = connection.recv(1024)
            if data == None:
                break
            if data.startswith(b"*1\r\n$4\r\nPING"):
                self.handle_ping(connection)
            elif data.startswith(b"*2\r\n$4\r\nECHO"):
                self.handle_echo(connection, data)
            elif data.startswith(b"*3\r\n$3\r\nSET"):
                self.handle_set(connection, data)
            elif data.startswith(b"*2\r\n$3\r\nGET"):
                self.handle_get(connection, data)
            else:
                connection.sendall(b"-ERR unknown command\r\n")
        connection.close()


    def handle_ping(self, connection):
        connection.sendall(b"+PONG\r\n")


    def handle_echo(self, connection, data):
        parts = data.split(b"\r\n")
        # Find the index of the argument length and value
        try:
            # Find the index of the second '$'
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) >= 2:
                arg_index = dollar_indices[1] + 1
                message = parts[arg_index]
                response = b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
                connection.sendall(response)
            else:
                connection.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")
        except Exception:
            connection.sendall(b"-ERR wrong number of arguments for 'echo' command\r\n")


    def handle_set(self,connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) >= 3:
                key_index = dollar_indices[1] + 1
                value_index = dollar_indices[2] + 1
                key = parts[key_index]
                value = parts[value_index]
                # Store key and value in Handle's dictionary
                self.dictionary[key] = value
                connection.sendall(b"+OK\r\n")
            else:
                connection.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")
        except Exception:
            connection.sendall(b"-ERR wrong number of arguments for 'set' command\r\n")


    def handle_get(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) >= 2:
                key_index = dollar_indices[1] + 1
                key = parts[key_index]
                value = self.dictionary.get(key, None)
                if value is None:
                    connection.sendall(b"$-1\r\n")
                    return
                else:
                    response = b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                    connection.sendall(response)
            else:
                connection.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
        except Exception:
            connection.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    handler = Handler()
    while True:
        connection, _ = server_socket.accept() # wait for client
        client_thread = threading.Thread(target=handler.handle, args=(connection,))
        client_thread.start()


if __name__ == "__main__":
    main()
