import socket
import configparser
from client import Operation
import re
import sys
from create_instance import CloudAPI


def tokenize(message):
    message = message.lower()
    all_words = re.findall('[a-z0-9]+', message)
    return all_words


def map_fn_wc(*argv):
    split1, mapper_number, serverip = argv
    total_docs = len(argv) - 2

    map_num_and_book_num = str(mapper_number)

    for i in range(total_docs):
        map_num_and_book_num += "_" + str(i)

    m = Operation()
    m.connect(serverip)

    # mapper tells it's mapper number and doc number to the server for file creation
    m.command(map_num_and_book_num)

    for word in tokenize(split1):
        # SET word_docnum 1\r\n
        # 1\r\n
        send_to_client = "SET " + word + "_doc0" + " 1" + "\r\n" + "1" + "\r\n"
        m.command(send_to_client)

    m.disconnect()
    return


def map_fn_ii(*argv):
    split1, split2, mapper_number, serverip = argv
    total_docs = len(argv) - 2

    map_num_and_book_num = str(mapper_number)

    for i in range(total_docs):
        map_num_and_book_num += "_" + str(i)

    m = Operation()
    m.connect(serverip)

    # mapper tells it's mapper number and doc number to the server for file creation
    m.command(map_num_and_book_num)

    for word in tokenize(split1):
        # SET word_doc(number) 1\r\n
        # 1\r\n
        send_to_client = "SET " + word + "_doc0" + " 1" + "\r\n" + "1" + "\r\n"
        m.command(send_to_client)

    for word in tokenize(split2):
        send_to_client = "SET " + word + "_doc1" + " 1" + "\r\n" + "1" + "\r\n"
        m.command(send_to_client)

    m.disconnect()
    return


class Connect:
    def __init__(self):
        Config = configparser.ConfigParser()
        Config.read("config.ini")
        Config.sections()
        s = socket.socket()

        # print("Entered the mapper")
        kv = CloudAPI()
        instances = kv.all_instances()
        for instance in instances:
            if instance['name'] == "kv-server":
                serverip = kv.get_ip(instance)
            if instance['name'] == "master-node":
                host = kv.get_ip(instance)

        # print("Got server IP:", serverip)
        # print("Got master node's IP:", host)

        port = int(Config['A3']['port'])
        operation = Config['A3']['operation']

        s.connect((host, port))
        # Connection between mapper and master has been established

        ack = s.send(str.encode("Mapper Ready"))  # Mapper sending ack that it's ready

        chunk1 = str(s.recv(1000000000), "utf-8")  # received 1st chunk from master node
        ack2 = s.send(str.encode("Mappers Received the data"))
        # print("Ye chunk-1 hai:", chunk1)

        chunk2 = str(s.recv(1000000000), "utf-8")  # received 2nd chunk from master node
        ack3 = s.send(str.encode("Mappers Received the data"))
        # print("Ye chunk-2 hai:", chunk2)

        mapper_number = str(s.recv(100), "utf-8")  # received the mapper number from master node
        ack4 = s.send(str.encode("Mappers Received its number"))
        # print("This is my number:", mapper_number)

        if operation == "WC":
            map_fn_wc(chunk1, mapper_number, serverip)
        elif operation == "II":
            map_fn_ii(chunk1, chunk2, mapper_number, serverip)

        s.send(str.encode("Mappers Task Done"))


if __name__ == "__main__":
    ma = Connect()
