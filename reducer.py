import socket
import configparser
from client import Operation
import sys
import time
from create_instance import CloudAPI
import os


class Connect:
    def __init__(self):
        Config = configparser.ConfigParser()
        Config.read("config.ini")
        Config.sections()
        self.s = socket.socket()
        # print("Entered the reducer")

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
        num_reducers = int(Config['A3']['n_reducer'])
        self.s.connect((host, port))
        # Connection between reducer and master has been established

        operation = Config['A3']['operation']  # WC: Word Count || II: Inverted Index

        self.s.send(str.encode("Reducer Ready"))  # Reducer sending ack that it's ready
        reducer_number = str(self.s.recv(100), "utf-8")  # received reducer number from master node

        self.r = Operation()
        self.r.connect(serverip)

        self.r.command(str(reducer_number))
        self.words = self.r.get_all(str(reducer_number)).split(" ")

        if operation == "WC":
            to_send = ""
            for word in self.words:
                # decides which reducer will read which words, similar to hash function
                magic_number = len(word) % num_reducers
                if magic_number == reducer_number:
                    # Running the reduce function
                    returned = self.reduce_fn_wc(word)
                    to_send += returned + "\n"
                else:
                    continue
            self.s.send(bytes(to_send, "utf-8"))
        elif operation == "II":
            to_send = ""
            for word in self.words:
                # decides which reducer will read which words, similar to hash function
                magic_number = len(word) % num_reducers
                if magic_number == reducer_number:
                    # Running the reduce function
                    returned = self.reduce_fn_ii(word)
                    to_send += returned + "\n"
                else:
                    continue
            self.s.send(bytes(to_send, "utf-8"))

        self.r.disconnect()
        self.s.send(str.encode("Reducer Job Done"))

    def reduce_fn_wc(self, word):
        # print("Word:", word)
        k = "GET " + word + "\r\n"
        count = self.r.command(k)

        # print("Count with word returned:", count)

        splitted_output = count.split("_")

        # print("S:", splitted_output)

        # word count processing
        to_write = word + " " + str(sum([int(counts) for counts in splitted_output[1]]))

        return to_write

    def reduce_fn_ii(self, word):
        k = "GET " + word + "\r\n"

        doc_count = self.r.command(k)
        splitted_output = doc_count.split("_")

        # inverted index processing
        to_write = word + " " + splitted_output[0] + " " + str(sum([int(counts) for counts in splitted_output[1]])) + \
                   " " + splitted_output[2] + " " + str(sum([int(counts) for counts in splitted_output[3]]))

        return to_write


if __name__ == "__main__":
    re = Connect()
