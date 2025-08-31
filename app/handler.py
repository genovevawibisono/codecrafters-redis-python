import time
import threading

class Handler:
    def __init__(self):
        # Store value and expiry (None or timestamp in seconds)
        self.dictionary = dict()
        self.lock = threading.Lock()

    def handle(self, connection):
        while True:
            data = connection.recv(1024)
            if not data:
                break
            if data.startswith(b"*1\r\n$4\r\nPING"):
                self.handle_ping(connection)
            elif data.startswith(b"*2\r\n$4\r\nECHO"):
                self.handle_echo(connection, data)
            elif data.startswith(b"*3\r\n$3\r\nSET") or data.startswith(b"*5\r\n$3\r\nSET"):
                self.handle_set(connection, data)
            elif data.startswith(b"*2\r\n$3\r\nGET"):
                self.handle_get(connection, data)
            elif b"\r\n$5\r\nRPUSH\r\n" in data:
                self.handle_rpush(connection, data)
            elif b"\r\n$6\r\nLRANGE\r\n" in data:
                self.handle_lrange(connection, data)
            elif b"\r\n$5\r\nLPUSH\r\n" in data:
                self.handle_lpush(connection, data)
            elif data.startswith(b"*2\r\n$4\r\nLLEN"):
                self.handle_llen(connection, data)
            elif b"\r\n$4\r\nLPOP\r\n" in data or data.startswith(b"*2\r\n$4\r\nLPOP"):
                self.handle_lpop(connection, data)
            elif b"BLPOP" in data:
                self.handle_blpop(connection, data)
            elif b"TYPE" in data:
                self.handle_type(connection, data)
            else:
                connection.sendall(b"-ERR unknown command\r\n")
        connection.close()

    def handle_ping(self, connection):
        connection.sendall(b"+PONG\r\n")

    def handle_echo(self, connection, data):
        parts = data.split(b"\r\n")
        try:
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

    def handle_set(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) >= 3:
                key_index = dollar_indices[1] + 1
                value_index = dollar_indices[2] + 1
                key = parts[key_index]
                value = parts[value_index]
                expiry = None
                # Check for PX argument
                if len(dollar_indices) >= 5:
                    px_index = dollar_indices[3] + 1
                    px_value_index = dollar_indices[4] + 1
                    if parts[px_index].upper() == b"PX":
                        try:
                            ms = int(parts[px_value_index])
                            expiry = time.time() + ms / 1000.0
                        except Exception:
                            connection.sendall(b"-ERR PX value is not an integer\r\n")
                            return
                # Store value and expiry (None if not set)
                self.dictionary[key] = (value, expiry)
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
                entry = self.dictionary.get(key, None)
                if entry is None:
                    connection.sendall(b"$-1\r\n")
                    return
                value, expiry = entry
                # Check expiry
                if expiry is not None and time.time() > expiry:
                    # Key expired, remove it
                    del self.dictionary[key]
                    connection.sendall(b"$-1\r\n")
                    return
                response = b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                connection.sendall(response)
            else:
                connection.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")
        except Exception:
            connection.sendall(b"-ERR wrong number of arguments for 'get' command\r\n")

    def handle_rpush(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) < 3:
                connection.sendall(b"-ERR wrong number of arguments for 'rpush' command\r\n")
                return
            key_index = dollar_indices[1] + 1
            key = parts[key_index]
            # All remaining $ indices are values
            values = [parts[dollar_indices[i] + 1] for i in range(2, len(dollar_indices))]
            values = [v for v in values if v != b'']
            entry = self.dictionary.get(key)
            if entry is not None:
                current_value, expiry = entry
                if not isinstance(current_value, list):
                    connection.sendall(b"-ERR wrong type\r\n")
                    return
                current_value.extend(values)
                lst = current_value
            else:
                lst = values
                expiry = None
            self.dictionary[key] = (lst, expiry)
            response = b":" + str(len(lst)).encode() + b"\r\n"
            connection.sendall(response)
        except Exception:
            connection.sendall(b"-ERR error processing 'rpush' command\r\n")

    def handle_lrange(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) < 4:
                connection.sendall(b"-ERR wrong number of arguments for 'lrange' command\r\n")
                return
            key_index = dollar_indices[1] + 1
            start_index = dollar_indices[2] + 1
            end_index = dollar_indices[3] + 1
            key = parts[key_index]
            try:
                start = int(parts[start_index])
                end = int(parts[end_index])
            except ValueError:
                connection.sendall(b"-ERR start or end is not an integer\r\n")
                return
            entry = self.dictionary.get(key)
            if entry is None:
                connection.sendall(b"*0\r\n")
                return
            value, expiry = entry
            if not isinstance(value, list):
                connection.sendall(b"-ERR wrong type\r\n")
                return
            # Handle negative indices
            if start < 0:
                start += len(value)
            if end < 0:
                end += len(value)
            # Adjust end to be inclusive
            end += 1
            # Slice the list safely
            sliced = value[max(0, start):min(len(value), end)]
            response = b"*" + str(len(sliced)).encode() + b"\r\n"
            for item in sliced:
                response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
            connection.sendall(response)
        except Exception:
            connection.sendall(b"-ERR error processing 'lrange' command\r\n")

    def handle_lpush(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) < 3:
                connection.sendall(b"-ERR wrong number of arguments for 'lpush' command\r\n")
                return
            key_index = dollar_indices[1] + 1
            key = parts[key_index]
            # All remaining $ indices are values
            values = []
            for i in range(2, len(dollar_indices)):
                value_index = dollar_indices[i] + 1
                if value_index < len(parts):
                    values.append(parts[value_index])
            # Store or update the list in the dictionary
            entry = self.dictionary.get(key)
            if entry is not None:
                current_value, expiry = entry
                if not isinstance(current_value, list):
                    connection.sendall(b"-ERR wrong type\r\n")
                    return
                for v in values:
                    current_value.insert(0, v)
                lst = current_value
            else:
                lst = list(reversed(values))
                expiry = None
            self.dictionary[key] = (lst, expiry)
            # Respond with the length of the list
            response = b":" + str(len(lst)).encode() + b"\r\n"
            connection.sendall(response)
        except Exception:
            connection.sendall(b"-ERR error processing 'lpush' command\r\n")

    def handle_llen(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) < 2:
                connection.sendall(b"-ERR wrong number of arguments for 'llen' command\r\n")
                return
            key_index = dollar_indices[1] + 1
            key = parts[key_index]
            entry = self.dictionary.get(key)
            if entry is None:
                connection.sendall(b":0\r\n")
                return
            value, expiry = entry
            # Check expiry for lists as well
            if expiry is not None and time.time() > expiry:
                del self.dictionary[key]
                connection.sendall(b":0\r\n")
                return
            if not isinstance(value, list):
                connection.sendall(b"-ERR wrong type\r\n")
                return
            response = b":" + str(len(value)).encode() + b"\r\n"
            connection.sendall(response)
        except Exception:
            connection.sendall(b"-ERR error processing 'llen' command\r\n")

    def handle_lpop(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) < 2:
                connection.sendall(b"-ERR wrong number of arguments for 'lpop' command\r\n")
                return
            key_index = dollar_indices[1] + 1
            key = parts[key_index]
            count = None
            if (len(dollar_indices) >= 3):
                count_index = dollar_indices[2] + 1
                try:
                    count = int(parts[count_index])
                    if count <= 0:
                        connection.sendall(b"-ERR count must be positive\r\n")
                        return
                except ValueError:
                    connection.sendall(b"-ERR count is not an integer\r\n")
                    return
            entry = self.dictionary.get(key)
            if entry is None:
                connection.sendall(b"$-1\r\n")
                return
            value, expiry = entry
            if expiry is not None and time.time() > expiry:
                del self.dictionary[key]
                connection.sendall(b"$-1\r\n")
                return
            if not isinstance(value, list):
                connection.sendall(b"-ERR wrong type\r\n")
                return
            if count is None:
                if len(value) == 0:
                    connection.sendall(b"$-1\r\n")
                    return
                first_element = value.pop(0)
                self.dictionary[key] = (value, expiry)
                response = b"$" + str(len(first_element)).encode() + b"\r\n" + first_element + b"\r\n"
                connection.sendall(response)
            else:
                if count <= 0 or len(value) == 0:
                    connection.sendall(b"*0\r\n")
                    return
                popped = []
                for _ in range(min(count, len(value))):
                    popped.append(value.pop(0))
                self.dictionary[key] = (value, expiry)
                response = b"*" + str(len(popped)).encode() + b"\r\n"
                for item in popped:
                    response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
                connection.sendall(response)
        except Exception:
            connection.sendall(b"-ERR error processing 'lop' command\r\n")

    def handle_blpop(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) < 3:
                connection.sendall(b"-ERR wrong number of arguments for 'blpop' command\r\n")
                return
            key_indices = [dollar_indices[i] + 1 for i in range(1, len(dollar_indices) - 1)]
            keys = [parts[i] for i in key_indices]
            timeout_idx = dollar_indices[-1] + 1
            try:
                timeout = float(parts[timeout_idx])
            except Exception:
                connection.sendall(b"-ERR timeout is not a float\r\n")
                return
            start_time = time.time()
            while True:
                for key in keys:
                    with self.lock:
                        entry = self.dictionary.get(key)
                        if entry is not None:
                            value, expiry = entry
                            if expiry is not None and time.time() > expiry:
                                del self.dictionary[key]
                                continue
                            if not isinstance(value, list):
                                connection.sendall(b"-ERR wrong type\r\n")
                                return
                            if len(value) > 0:
                                first_element = value.pop(0)
                                self.dictionary[key] = (value, expiry)
                                response = (
                                    b"*2\r\n"
                                    + b"$" + str(len(key)).encode() + b"\r\n" + key + b"\r\n"
                                    + b"$" + str(len(first_element)).encode() + b"\r\n" + first_element + b"\r\n"
                                )
                                connection.sendall(response)
                                return
                if timeout > 0 and (time.time() - start_time) >= timeout:
                    connection.sendall(b"*-1\r\n")
                    return
                time.sleep(0.01)
        except Exception:
            connection.sendall(b"-ERR error processing 'blpop' command\r\n")

    def handle_type(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) < 2:
                connection.sendall(b"-ERR wrong number of arguments for 'type' command\r\n")
                return
            key_index = dollar_indices[1] + 1
            key = parts[key_index]
            entry = self.dictionary.get(key)
            if entry is None:
                connection.sendall(b"+none\r\n")
                return
            value, expiry = entry
            if expiry is not None and time.time() > expiry:
                del self.dictionary[key]
                connection.sendall(b"+none\r\n")
                return
            if isinstance(value, list):
                connection.sendall(b"+list\r\n")
            else:
                connection.sendall(b"+string\r\n")
        except Exception:
            connection.sendall(b"-ERR error processing 'type' command\r\n")