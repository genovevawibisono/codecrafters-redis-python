import time
import threading
import uuid

class Handler:
    def __init__(self):
        # Store value and expiry (None or timestamp in seconds)
        self.dictionary = dict()
        self.lock = threading.Lock()
        self.streams = {}

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
            elif b"XADD" in data:
                self.handle_xadd(connection, data)
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
            
            # Check dictionary first
            value, expiry = self.__search_dictionary(key)
            if value is not None:
                if isinstance(value, list):
                    connection.sendall(b"+list\r\n")
                else:
                    connection.sendall(b"+string\r\n")
                return
            
            # Check streams
            stream_data = self.__search_stream(key)
            if stream_data is not None:
                connection.sendall(b"+stream\r\n")
                return
            
            # Key doesn't exist
            connection.sendall(b"+none\r\n")
        except Exception:
            connection.sendall(b"-ERR error processing 'type' command\r\n")

    def __search_dictionary(self, key):
        entry = self.dictionary.get(key)
        if entry is None:
            return None, None
        value, expiry = entry
        if expiry is not None and time.time() > expiry:
            del self.dictionary[key]
            return None, None
        return value, expiry
    
    def __search_stream(self, stream_name):
        if stream_name in self.streams:
            return self.streams[stream_name]
        return None

    def handle_xadd(self, connection, data):
        parts = data.split(b"\r\n")
        try:
            dollar_indices = [i for i, part in enumerate(parts) if part.startswith(b"$")]
            if len(dollar_indices) < 4:
                connection.sendall(b"-ERR wrong number of arguments for 'xadd' command\r\n")
                return
            
            stream_index = dollar_indices[1] + 1
            id_index = dollar_indices[2] + 1
            stream_name = parts[stream_index]
            entry_id = parts[id_index]
            
            # Handle different ID formats
            if b"-*" in entry_id:
                # Semi auto-generated (timestamp-*)
                entry_id = self.__validation_semi_auto_generated(connection, stream_name, entry_id)
                if entry_id is None:  # Validation failed
                    return
            elif entry_id != b'*':
                # Explicit ID validation
                if not self.__validation_non_auto_generated(connection, stream_name, entry_id):
                    return
            
            # Field-value parsing
            field_value_pairs = {}
            for i in range(3, len(dollar_indices), 2):
                if i + 1 < len(dollar_indices):
                    field_index = dollar_indices[i] + 1
                    value_index = dollar_indices[i + 1] + 1
                    field = parts[field_index]
                    value = parts[value_index]
                    field_value_pairs[field] = value
            
            if stream_name not in self.streams:
                self.streams[stream_name] = []
            
            if entry_id == b'*':
                entry_id = uuid.uuid4().hex.encode()
            
            entry = (entry_id, field_value_pairs)
            self.streams[stream_name].append(entry)
            response = b"$" + str(len(entry_id)).encode() + b"\r\n" + entry_id + b"\r\n"
            connection.sendall(response)
            
        except Exception:
            connection.sendall(b"-ERR error processing 'xadd' command\r\n")

    def __validation_non_auto_generated(self, connection, stream_name, entry_id):
        if not self.__validate_stream_id(entry_id):
            connection.sendall(b"-ERR invalid stream ID\r\n")
            return False
        
        if not self.__check_id_greater_than_min(entry_id):
            connection.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
            return False
        
        if stream_name not in self.streams:
            self.streams[stream_name] = []
        
        if len(self.streams[stream_name]) > 0:
            last_entry_id = self.streams[stream_name][-1][0]
            if not self.__check_id_greater_than(entry_id, last_entry_id):
                connection.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return False
        
        return True

    def __validation_semi_auto_generated(self, connection, stream_name, entry_id):
        timestamp_part = entry_id.split(b"-", 1)[0]
        if not self.__check_timestamp(timestamp_part):
            connection.sendall(b"-ERR invalid stream ID\r\n")
            return None
        
        timestamp = int(timestamp_part)
        next_sequence = self.__get_next_sequence_for_timestamp(stream_name, timestamp)
        generated_id = timestamp_part + b'-' + str(next_sequence).encode()
        return generated_id

    def __check_id_greater_than(self, id1, id2):
        if self.__validate_stream_id(id1) is False or self.__validate_stream_id(id2) is False:
            return False
        t1, s1 = id1.split(b"-", 1)
        t2, s2 = id2.split(b"-", 1)
        if int(t1) > int(t2):
            return True
        elif int(t1) == int(t2) and int(s1) > int(s2):
            return True 
        return False

    def __validate_stream_id(self, id):
        parsed_id = id.split(b"-", 1)
        if len(parsed_id) != 2:
            return False
        try:
            # In milliseconds
            timestamp = parsed_id[0]
            # Sequence number
            sequence_number = parsed_id[1]
            validate_timestamp_res = self.__check_timestamp(timestamp)
            validate_sequence_number_res = self.__check_sequence_number(sequence_number)
            return validate_timestamp_res and validate_sequence_number_res
        except Exception:
            return False
        
    def __check_sequence_number(self, sequence_number):
        try:
            int(sequence_number)
        except Exception:
            return False
        return True

    def __check_timestamp(self, timestamp):
        try:
            int(timestamp)
        except Exception:
            return False
        return True
    
    def __check_id_greater_than_min(self, id):
        return self.__check_id_greater_than(id, b"0-0")
        
    def __get_next_sequence_for_timestamp(self, stream_name, timestamp):
        if stream_name not in self.streams:
            # For timestamp 0, start with sequence 1, otherwise start with 0
            return 1 if timestamp == 0 else 0
        
        # Find the highest sequence number for this timestamp
        max_sequence = -1
        for entry_id, _ in self.streams[stream_name]:
            try:
                entry_timestamp, entry_sequence = entry_id.split(b'-')
                if int(entry_timestamp) == timestamp:
                    max_sequence = max(max_sequence, int(entry_sequence))
            except (ValueError, IndexError):
                continue
        
        # If no entries found for this timestamp
        if max_sequence == -1:
            return 1 if timestamp == 0 else 0
        
        # Return next sequence number
        return max_sequence + 1
            