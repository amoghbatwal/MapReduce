import socket
import json
import threading
import sys
import os

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("0.0.0.0", 8080))
s.listen(20)


def handle(conn_addr):
    print("Someone Connected")
    clientsocket, address = conn_addr
    print(f"Connection from {address} has been established!")

    all_info = clientsocket.recv(20).decode("utf-8")
    clientsocket.send("received".encode("utf-8"))

    split_info = all_info.split("_")

    if len(split_info) != 1:
        mapper_number = split_info[0]
        if mapper_number not in master_dict.keys():
            master_dict[mapper_number] = {}  # {0: {}, 1:  {} }

    if len(split_info) > 1:
        mapper_number = split_info[0]
        for book_number in range(len(split_info) - 1):
            doc = "doc" + str(book_number)
            master_dict[mapper_number][doc] = {}

    '''
    For word count:
    master_dict = { 
                    'mapper0': {'doc0': {words go here} }, 
                    'mapper1': {'doc0': {words go here} }, 
                    'mapper2': {'doc0': {words go here} } 
                    }
                    
    For inverted index:
    master_dict = {
                    'mapper0': {'doc0': {words go here}, 'doc1': {words go here}}, 
                    'mapper1': {'doc0': {words go here}, 'doc1': {words go here}}, 
                    'mapper2': {'doc0': {words go here}, 'doc1': {words go here}}}
    '''

    while True:
        try:
            cmd_from_client = clientsocket.recv(1234).decode("utf-8")

            if cmd_from_client == 'DISCONNECT':
                clientsocket.close()
                raise ConnectionResetError

            # GET <key>\r\n
            if cmd_from_client.split(" ")[0] == 'GET':

                key = cmd_from_client.split(" ")[1].split("\r\n")[0]

                temp_dic = {}
                for outerkey in master_dict:
                    for innerkey in master_dict[outerkey]:
                        temp_dic[innerkey] = []

                for outerkey in master_dict:
                    for innerkey in master_dict[outerkey]:
                        if key in master_dict[outerkey][innerkey].keys():
                            temp_dic[innerkey] += master_dict[outerkey][innerkey][key]

                value = ""
                for k, v in temp_dic.items():
                    value += k + "_" + "".join(v) + "_"

                msg = value  # doc0_3_doc1_7
                clientsocket.send(msg.encode("utf-8"))

            # GA <key>\r\n
            elif cmd_from_client.split(" ")[0] == 'GA':
                '''
                    For inverted index
                    master_dict = {
                                    'mapper0': {'doc0': {word1 : []}, 'doc1': {}}, 
                                    'mapper1': {'doc0': {}, 'doc1': {}}, 
                                    'mapper2': {'doc0': {}, 'doc1': {}}}
                '''
                for outerkey in master_dict:
                    for innerkey in master_dict[outerkey]:
                        all_unique_keys.update(master_dict[outerkey][innerkey].keys())

                all_unique_keys_str = " ".join(all_unique_keys)

                clientsocket.send(all_unique_keys_str.encode("utf-8"))

            # SET <key> <value-size-bytes> \r\n
            # <value> \r\n
            elif cmd_from_client.split("\r\n")[0].split(" ")[0] == 'SET':
                my_data = cmd_from_client.split("\r\n")
                key_docnumber = (my_data[0].split(" "))[1]
                key_docnumber = key_docnumber.split("_")
                key = key_docnumber[0]
                docnumber = key_docnumber[1]

                size = (my_data[0].split(" "))[2]
                value = my_data[1]

                if len(value.encode("utf-8")) > int(size):  # size greater
                    msg = 'NOT-STORED\r\n'
                    clientsocket.send(msg.encode("utf-8"))
                else:
                    outerkey = mapper_number
                    innerkey = docnumber
                    innerinnerkey = key

                    if innerinnerkey not in master_dict[outerkey][innerkey]:
                        master_dict[outerkey][innerkey][innerinnerkey] = [value]
                    else:
                        master_dict[outerkey][innerkey][innerinnerkey].append(value)

                    msg = 'STORED\r\n'
                    clientsocket.send(msg.encode("utf-8"))

        except ConnectionResetError:
            print(f"Connection closed by client with details: {address}")
            sys.exit()


master_dict = {}
all_unique_keys = set()

while True:
    threading.Thread(target=handle, args=(s.accept(),)).start()
