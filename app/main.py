import socket  # noqa: F401
import threading

def handle(connection):
    while True:
        data = connection.recv(1024)
        if data == None:
            break
        if data.startswith(b"*1\r\n$4\r\nPING"):
            handle_ping(connection)
        elif data.startswith(b"*2\r\n$4\r\nECHO"):
            handle_echo(connection, data)
        else:
            connection.sendall(b"-ERR unknown command\r\n")
    connection.close()


def handle_ping(connection):
    connection.sendall(b"+PONG\r\n")


def handle_echo(connection, data):
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
