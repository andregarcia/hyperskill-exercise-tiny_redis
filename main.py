# Write your solution here
import abc
import queue
import socket
import sys
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass
from threading import Thread
from typing import List, DefaultDict

hostname = '127.0.0.1'
port = 6379
addr = (hostname, port)

server_addr = ('0.0.0.0', 6379)


STOP_SERVER_ON_EXIT_COMMAND = False

class Command(abc.ABC):
    @abc.abstractmethod
    def str(self) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def is_command(cls, command: str) -> bool:
        pass

    @classmethod
    @abc.abstractmethod
    def to_command(cls, s: str) -> 'Command':
        pass

    def str_encoded(self) -> str:
        return self.str().encode()

class UnknownCommand(Command):
    def __init__(self, s: str):
        self.s = s

    def str(self) -> str:
        return self.s

    @classmethod
    def is_command(cls, command: str) -> bool:
        return False

    @classmethod
    def to_command(cls, s: str) -> 'Command':
        return UnknownCommand(s)

def command_item_to_str(s: str):
    return  '$' + str(len(s)) + '\r\n' + \
             s + '\r\n'

def command_header_to_str(command_name: str, n_args: int):
    return '*' + str(n_args + 1) + '\r\n' + \
                  command_item_to_str(command_name)

def command_to_str(arr: List[str]) -> str:
    command_str = command_header_to_str(arr[0], len(arr) - 1)
    for a in arr[1:]:
        if not isinstance(a, (str, int)):
            raise Exception("Unexpected type")
        command_str += command_item_to_str(str(a))
    return command_str

def extract_command_name_part_from_str(command_str: str) -> str:
    return '\r\n'.join(command_str.split('\r\n')[1:3]) + '\r\n'

def extract_command_parameters_from_str(command_str: str) -> str:
    return '\r\n'.join(command_str.split('\r\n')[3:]) + '\r\n'

def extract_parameters_from_str(command_str: str) -> List[str]:
    return command_str.split('\r\n')[4::2]

class PingCommand(Command):
    def str(self) -> str:
        return self._str()

    @staticmethod
    def _str():
        return command_to_str(['PING'])

    @classmethod
    def to_command(cls, s: str) -> 'Command':
        return PingCommand()

    @classmethod
    def is_command(cls, command: str) -> bool:
        return command == cls._str()

class ExitCommand(Command):
    @classmethod
    def is_command(cls, command: str) -> bool:
        return command == ExitCommand._str()
    @classmethod
    def to_command(cls, s: str) -> 'Command':
        return ExitCommand()

    def str(self) -> str:
        return self._str()

    @staticmethod
    def _str():
        return command_to_str(['EXIT'])

class EchoCommand(Command):
    def __init__(self, message):
        self.message = message

    def str(self) -> str:
        return command_to_str(['ECHO', self.message] if self.message else ['ECHO'])

    @classmethod
    def is_command(cls, c: str) -> bool:
        return extract_command_name_part_from_str(c) == command_item_to_str('ECHO')

    @classmethod
    def to_command(cls, s: str) -> 'Command':
        parameters = extract_parameters_from_str(s)
        if len(parameters) != 1:
            raise Exception("wrong number of arguments for ECHO")
        return EchoCommand(parameters[0])

class Entry:
    def __init__(self, value:str, expire_in: int = None):
        self.value, self.expire_in = value, expire_in

ENTRIES = {}

class SetCommand(Command):
    def __init__(self, key, value, expire_in: int = None):
        self.key, self.value, self.expire_in = key, value, expire_in
    @classmethod
    def is_command(cls, command: str) -> bool:
        return extract_command_name_part_from_str(command) == command_item_to_str('SET')
    @classmethod
    def to_command(cls, s: str) -> 'Command':
        parameters = extract_parameters_from_str(s)
        if len(parameters) not in (2, 4):
            raise Exception("Set command has wrong number of parameters")
        expire_in = None
        if len(parameters) == 4:
            if parameters[2] != 'EX':
                raise Exception('Expected EX parameter, found ' + parameters[3])
            expire_in = int(parameters[3])
            if expire_in < 0:
                raise Exception("EXP must be positive")
        return SetCommand(parameters[0], parameters[1], expire_in)
    def str(self) -> str:
        return command_to_str(['SET', self.key, self.value] if self.expire_in is None else ['SET', self.key, self.value, 'EX', self.expire_in])


class GetCommand(Command):
    def str(self) -> str:
        return command_to_str(['GET', self.key])
    @classmethod
    def is_command(cls, command: str) -> bool:
        return extract_command_name_part_from_str(command) == command_item_to_str('GET')
    @classmethod
    def to_command(cls, s: str) -> 'Command':
        parameters = extract_parameters_from_str(s)
        if len(parameters) != 1:
            raise Exception("Get command has wrong number of parameters")
        return GetCommand(parameters[0])
    def __init__(self, key):
        self.key = key

class DelCommand(Command):
    def str(self) -> str:
        return command_to_str(['DEL', self.key])
    @classmethod
    def is_command(cls, command: str) -> bool:
        return extract_command_name_part_from_str(command) == command_item_to_str('DEL')
    @classmethod
    def to_command(cls, s: str) -> 'Command':
        parameters = extract_parameters_from_str(s)
        if len(parameters) != 1:
            raise Exception("Del command has wrong number of parameters")
        return DelCommand(parameters[0])
    def __init__(self, key: str):
        self.key = key


class Subscription:
    def __init__(self, client_connection: 'ClientConnection', topic: str):
        self.client_connection = client_connection
        self.topic = topic


class Subscriptions:
    def __init__(self):
        self.subscribed_clients = defaultdict(list) # type: DefaultDict[str,List[Subscription]]
    def subscribe(self, subscribe_command: 'SubscribeCommand', client: 'ClientConnection'):
        self._subscribe(subscribe_command.topic, client)
    def _subscribe(self, topic: str, client: 'ClientConnection'):
        self.subscribed_clients[topic].append(Subscription(client, topic))
    def publish(self, publish_command: 'MessageResponse'):
        topic = publish_command.topic
        command_str = publish_command.str().encode()
        for subscription in self.subscribed_clients[topic]:
            subscription.client_connection.conn.send(command_str)

SUBSCRIPTIONS = Subscriptions()


class SubscribeCommand(Command):
    def __init__(self, topic: str):
        self.topic = topic
    def str(self) -> str:
        return command_to_str(['SUBSCRIBE', self.topic])
    @classmethod
    def is_command(cls, command: str) -> bool:
        return extract_command_name_part_from_str(command) == command_item_to_str('SUBSCRIBE')
    @classmethod
    def to_command(cls, s: str) -> 'Command':
        parameters = extract_parameters_from_str(s)
        if len(parameters) != 1:
            raise Exception("Subscribe command has wrong number of parameters")
        return SubscribeCommand(parameters[0])


class PublishCommand(Command):
    def str(self) -> str:
        return command_to_str(['PUBLISH', self.topic, self.message])
    @classmethod
    def is_command(cls, command: str) -> bool:
        return extract_command_name_part_from_str(command) == command_item_to_str('PUBLISH')
    @classmethod
    def to_command(cls, s: str) -> 'Command':
        parameters = extract_parameters_from_str(s)
        if len(parameters) != 2:
            raise Exception("Publish command has wrong number of parameters")
        return PublishCommand(parameters[0], parameters[1])
    def __init__(self, topic: str, message: str):
        self.topic, self.message = topic, message


commands = [PingCommand, ExitCommand, EchoCommand, SetCommand, GetCommand, DelCommand, SubscribeCommand, PublishCommand]


class QueueItem:
    def __init__(self, command: Command, server_client_inst: 'ServerClientInstance'):
        self.command = command
        self.server_client_inst = server_client_inst

    def get_client_connection(self):
        return self.server_client_inst.client_connection


class CommandParser:
    @staticmethod
    def parse_command_str(s: str):
        # print(f"received command [{s}]")
        if s is None:
            traceback.print_stack()
        # if requires_strip:
        #     s = s[1:].strip().strip('\r\n')
        for c_type in commands:
            if c_type.is_command(s):
                try:
                    command_obj = c_type.to_command(s)
                    return command_obj
                except:
                    continue
        return UnknownCommand(s)


class Client:
    def __init__(self, socket_: socket.socket):
        self.socket = socket_

    def send_command(self, command: Command):
        data = command.str().encode()
        self.socket.send(data)

    def receive_response(self):
        response = None
        while not response:
            response = self.socket.recv(1024)
        return response

@dataclass
class CommandResult:
    client_input: str
    client_command: Command
    server_response: str


class TermClientApplication:
    def __init__(self, client: Client):
        self.client = client
        self.client_command_app = ClientCommandApplication(client)

    def loop(self):
        while True:
            command_result = self.get_user_input_and_send()
            if not command_result:
                continue
            if isinstance(command_result.client_command, ExitCommand):
                break
            print(command_result.server_response.decode())

    def get_user_input_and_send(self) -> CommandResult:
        user_input = self.get_user_input()
        if not user_input:
            return None
        return self.client_command_app._send_command(user_input)

    @staticmethod
    def get_user_input():
        try:
            return input()
        except KeyboardInterrupt:
            # print("sending exit command")
            return '+EXIT'

class ClientCommandApplication:
    def __init__(self, client: Client):
        self.client = client

    def send_command(self, c: str):
        command_result = self._send_command(c)
        if not command_result:
            return
        if isinstance(command_result.client_command, ExitCommand):
            return
        print(command_result.server_response.decode())

    def _send_command(self, c: str) -> CommandResult:
        command = CommandParser.parse_command_str(c)
        self.client.send_command(command)

        server_response = None
        if not isinstance(command, ExitCommand):
            server_response = self.client.receive_response()
        return CommandResult(c, command, server_response)

    def _listen_and_print(self):
        def loop():
            message = None
            while not message:
                message = self.client.receive_response()
            print(message)

        t = Thread(target=loop, args=(), daemon=True)
        t.start()

class ServerResponse(abc.ABC):
    @abc.abstractmethod
    def str(self) -> str:
        pass

    @abc.abstractmethod
    def is_success(self) -> bool:
        pass

    def str_encoded(self) -> str:
        self.str().encode()

class PongResponse(ServerResponse):
    def str(self) -> str:
        return 'PONG'
    def is_success(self) -> bool:
        return True


class ExitResponse(ServerResponse):
    def str(self) -> str:
        return ''
    def is_success(self) -> bool:
        return True

class EchoResponse(ServerResponse):
    def is_success(self) -> bool:
        return True
    def __init__(self, message: str):
        self.message = message
    def str(self) -> str:
        return command_item_to_str(self.message)

class SetResponse(ServerResponse):
    def str(self) -> str:
        return "+OK\r\n"
    def is_success(self) -> bool:
        True

class DelResponse(ServerResponse):
    def __init__(self, key: str):
        self.key = key
    def str(self) -> str:
        return "+OK\r\n"
    def is_success(self) -> bool:
        return True

class SubscribeResponse(ServerResponse):
    def str(self) -> str:
        return "+OK\r\n"
    def is_success(self) -> bool:
        True

class PublishResponse(ServerResponse):
    def __init__(self, topic, message):
        self.topic, self.message = topic, message
    def str(self) -> str:
        return command_to_str(['PUBLISH', self.topic, self.message])
    def is_success(self) -> bool:
        return True

class MessageResponse(ServerResponse):
    def str(self) -> str:
        return command_to_str(['message', self.topic, self.message])
    def is_success(self) -> bool:
        True
    def __init__(self, topic: str, message: str):
        self.topic, self.message = topic, message


NULL_BULK_STRING = '$-1\r\n'

class GetResponse(ServerResponse):
    def __init__(self, key):
        self.key = key
        self.value = ENTRIES[self.key] if self.key in ENTRIES else None  # type: Entry
    def str(self) -> str:
        if self.value is None:
            return NULL_BULK_STRING
        if self.value.expire_in is not None and self.value.expire_in < time.time():
            return NULL_BULK_STRING
        else:
            return command_item_to_str(self.value.value)
    def is_success(self) -> bool:
        True


class UnknownCommandResponse(ServerResponse):
    def str(self) -> str:
        return '-Parsing error!\r\n'
    def is_success(self) -> bool:
        return False

class Server:
    def __init__(self, socket_: socket.socket):
        self.socket = socket_
        self.clients = []  # type: List['ClientConnection']
        self.client_instances = []

        self.queue = queue.Queue()

        self.wait_clients()

        # print('after wait clients')
        self.process_queue()

        # ToDo: check if needed
        # self.socket.shutdown(socket.SHUT_RDWR)

        # print('before conn.close clients')
        for cl in self.clients:
            cl.conn.close()

        # print('before final socked.close')
        self.socket.close()


    def wait_clients(self):
        def f():
            while True:
                try:
                    conn, addr = self.socket.accept()
                    client_conn = ClientConnection(conn, addr)
                    self.clients.append(client_conn)
                    self.start_client_loop(client_conn)
                    # print('started client loop')
                except Exception as e:
                    pass
        t = Thread(target=f, args=(), daemon=True)
        t.start()

    def start_client_loop(self, conn: 'ClientConnection'):
        server_client_inst = ServerClientInstance(conn, self.queue)
        server_client_inst.start()

    def process_queue(self):
        while True:
            try:
                item = self.queue.get()  # type: QueueItem
                # print(f'processing item type {item.command.__class__} str: {item.command.str()}')
                server_client_inst = item.server_client_inst
                # print('before exec')
                server_client_inst.execute(item)
                # print('before check to exit command')
                if isinstance(item.command, (ExitCommand,)):  # TEMP pt2: do not stop when ExitCommand
                    # print('exiting process queue loop')
                    if STOP_SERVER_ON_EXIT_COMMAND:
                        break
            except Exception as e:
                pass
                # print(f'exception in process queue {e.__class__}')
        # print('out of while True')

class ClientConnection:
    def __init__(self, conn: socket.socket, addr):
        self.conn = conn
        self.addr = addr

    def receive(self) -> str:
        response = None  # type: str
        while not response:
            response = self.conn.recv(1024)
        response = response.decode()
        return response

    def send(self, command: ServerResponse):
        if not command.str():
            return
        data = command.str().encode()
        self.conn.send(data)

class ServerClientInstance:
    MAX_LOOPS = 0
    def __init__(self, client_connection: ClientConnection, q: queue.Queue):
        self.client_connection = client_connection
        self.queue = q
        self.t = None # type: Thread

    def start(self):
        self.t = Thread(target=self.loop, args=(ServerClientInstance.MAX_LOOPS,), daemon=True)
        self.t.start()

    def loop(self, max_loops = None):
        try:
            loop_count = 0
            while True:
                loop_count += 1
                command = self.receive_and_enqueue()
                if isinstance(command, ExitCommand):
                    # print('is ExitCommand inside client loop')
                    break
                if max_loops and loop_count >= max_loops:
                    self.exit()
        except OSError as e:
            if e.errno == 9:
                pass
        except Exception as e:
            traceback.print_exc()
            # print(f'exception e loop {e.__class__}')
            self.exit()
        # print('finished client loop')

    def receive_and_enqueue(self) -> Command:
        command = self.client_connection.receive()
        command_obj = CommandParser.parse_command_str(command)
        self.queue.put(QueueItem(command_obj, self))
        return command

    def execute(self, queue_item: QueueItem):
        command_obj = queue_item.command

        response_obj = None
        if isinstance(command_obj, PingCommand):
            response_obj = PongResponse()
        elif isinstance(command_obj, ExitCommand):
            response_obj = ExitResponse()
        elif isinstance(command_obj, EchoCommand):
            response_obj = EchoResponse(command_obj.message)
        elif isinstance(command_obj, SetCommand):
            expire_date = command_obj.expire_in + int(time.time()) if command_obj.expire_in else None
            ENTRIES[command_obj.key] = Entry(command_obj.value, expire_date)
            response_obj = SetResponse()
        elif isinstance(command_obj, GetCommand):
            response_obj = GetResponse(command_obj.key)
        elif isinstance(command_obj, DelCommand):
            try:
                del ENTRIES[command_obj.key]
            except KeyError:
                pass
            response_obj = DelResponse(command_obj.key)
        elif isinstance(command_obj, SubscribeCommand):
            SUBSCRIPTIONS.subscribe(command_obj, queue_item.get_client_connection())
            response_obj = SubscribeResponse()
        elif isinstance(command_obj, PublishCommand):
            SUBSCRIPTIONS.publish(MessageResponse(command_obj.topic, command_obj.message))
            response_obj = PublishResponse(command_obj.topic, command_obj.message)
        else:
            response_obj = UnknownCommandResponse()

        self.client_connection.send(response_obj)

        if isinstance(command_obj, ExitCommand):
            self.client_connection.conn.close()
            #self.exit()

        return response_obj

    def exit(self):
        try:
            self.client_connection.conn.close()
        except:
            pass
            # print('error closing conn')
        raise SystemExit("stop server application")

def start_server_socket() -> socket.socket:
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(server_addr)
    server.listen()
    # server.connect(server_addr)
    return server

def start_client_socket() -> socket.socket:
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(1.0)
    client_socket.connect(addr)
    return client_socket


def __main_run_server():
    try:
        server_socket = start_server_socket()
        server = Server(server_socket)
        # print('after server init (exiting)')
        #server.wait_clients()
    except:
        server_socket.close()

def __main_run_sever_and_terminal_client():
    server_socket = start_server_socket()
    server = Server(server_socket)
    #server.wait_client()

    client_app = TermClientApplication(Client(start_client_socket()))
    client_app.loop()

def __main_run_server_and_automated_client():
    server_socket = start_server_socket()
    server = Server(server_socket)
    #server.wait_client()
    # time.sleep(1)

    print('init client app')
    client_app = ClientCommandApplication(Client(start_client_socket()))

    print('sending command')
    client_app.send_command('+PING\r\n')

    print('sending exit')
    client_app.send_command('+EXIT\r\n')

def __main_run_server_and_client_automated2():
    # __main_run_server()

    print('init client app')
    client_app1 = ClientCommandApplication(Client(start_client_socket()))

    print('sending command')
    client_app1.send_command('+PING\r\n')

    print('init client app')
    client_app2 = ClientCommandApplication(Client(start_client_socket()))

    print('sending exit')
    client_app2.send_command('+EXIT\r\n')

if __name__ == "__main__":
    __main_run_server()
    # print('after __main_run_server')
    # __main_run_server_and_client_automated2()
    # sys.exit(0)
