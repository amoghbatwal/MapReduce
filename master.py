import sys
import os
import socket
import configparser
from multiprocessing import Process
import time
from create_instance import CloudAPI

Config = configparser.ConfigParser()
Config.read("config.ini")
Config.sections()

# Argument 1: input file1 location
# Argument 2: input file2 location
# Argument 3: number of mappers
# Argument 4: number of reducer
# Argument 5: ip address of master node
# Argument 6: port number of master node
# Argument 7: operation to perform - word count or inverted index
# Argument 8: output location for word count
# Argument 9: output location for inverted index


# https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length/37414115
def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    final = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    final = [" ".join(chunk) for chunk in out]
    return final


def init_cluster(ip_address, port):
    try:
        global s
        s = socket.socket()
        s.bind((ip_address, port))
        s.listen(5)
        print("Cluster initialised")

    except socket.error as msg:
        print("SERVER_ERROR Socket Not Created:" + str(msg) + "\n" + "Trying again")
        init_cluster(ip_address, port)

    cid = 1
    return cid


def send_commands_mapper(conn_obj, chunk1, chunk2, mappernumber):
    rcvd_ack = str(conn_obj.recv(1024), "utf-8")
    # print("Mapper confirmation:", rcvd_ack)

    conn_obj.send(bytes(chunk1, "utf-8"))  # send data chunk to mapper
    rcvd_ack2 = str(conn_obj.recv(1024), "utf-8")
    # print("Chunk1 received confirmation:", rcvd_ack2)

    conn_obj.send(bytes(chunk2, "utf-8"))  # send data chunk to mapper
    rcvd_ack3 = str(conn_obj.recv(1024), "utf-8")
    # print("Chunk2 received confirmation:", rcvd_ack3)

    conn_obj.send(str.encode(str(mappernumber)))  # send mapper number to mapper
    rcvd_ack4 = str(conn_obj.recv(1024), "utf-8")

    rcvd_ack5 = str(conn_obj.recv(1024), "utf-8")
    # print("Mapping done confirmation:", rcvd_ack5)

    return "done"


def accepting_client_mapper(s, chunk1, chunk2, mappernumber):
    while True:
        conn_obj, address = s.accept()  # executes until it accepts a connection
        print("Connection established with Mapper with IP " + address[0] + " and port " + str(address[1]))
        job = send_commands_mapper(conn_obj, chunk1, chunk2, mappernumber)
        if job == "done":
            break
    return


def send_commands_reducer(conn_obj, reducernumber, operation):
    rcvd_ack = str(conn_obj.recv(30), "utf-8")
    # print("Connection acknowledgement:", rcvd_ack)

    conn_obj.send(str.encode(str(reducernumber)))  # send reducer number to reducer

    output = str(conn_obj.recv(100000000), "utf-8")
    # print("Output of reducer:", output)

    if operation == "WC":
        outfile_wc = Config['A3']['output_loc_wc']
        outputfile_wc = open(os.path.join(
                os.path.dirname(__file__), outfile_wc), 'a')
        outputfile_wc.write(output)
    elif operation == "II":
        outfile_ii = Config['A3']['output_loc_ii']
        outputfile_ii = open(os.path.join(
                os.path.dirname(__file__), outfile_ii), 'a')
        outputfile_ii.write(output)

    rcvd_ack2 = str(conn_obj.recv(1024), "utf-8")
    # print("Reducing done confirmation:", rcvd_ack2)

    return "done"


def accepting_client_reducer(s, reducernumber, operation):
    while True:
        conn_obj, address = s.accept()  # executes until it accepts a connection
        print("Connection established with Reducer with IP " + address[0] + " and port " + str(address[1]))
        job = send_commands_reducer(conn_obj, reducernumber, operation)
        if job == "done":
            break
    return


def mapper_spawn(mapper_name):
    per = CloudAPI()
    per.main("mapper-script.sh", mapper_name, wait=False)
    return


def reducer_spawn(reducer_name):
    red = CloudAPI()
    red.main("reducer-script.sh", reducer_name, wait=False)
    return


def run_mapred(file1, file2, num_mapper, num_reducer, operation):
    myfile1 = open(file1, 'r', encoding="utf8")
    myfile1 = myfile1.read()
    myfile1 = myfile1.split('\n')

    chunks1 = chunkIt(myfile1, num_mapper)

    myfile2 = open(file2, 'r', encoding="utf8")
    myfile2 = myfile2.read()
    myfile2 = myfile2.split('\n')

    chunks2 = chunkIt(myfile2, num_mapper)

    # Mapper operation begins
    processes = []

    name_of_mapper_instances = []

    for i in range(num_mapper):
        name_of_mapper_instances.append("mapper" + str(i))

    # print(name_of_mapper_instances)

    for i in range(num_mapper):
        p1 = Process(target=accepting_client_mapper, args=(s, chunks1[i], chunks2[i], i, ))
        p1.start()
        p2 = Process(target=mapper_spawn, args=(name_of_mapper_instances[i], ))
        p2.start()
        processes.append(p1)
        processes.append(p2)

    for p in processes:
        p.join()

    print("Mapper's job done")
    # Mapper operation ends

    # Deleting created mapper VMs
    deletion = CloudAPI()
    for instance in name_of_mapper_instances:
        op = deletion.delete_instance(instance)
        deletion.wait_for_operation(op['name'])

    time.sleep(15)
    # Reducer operation begins

    processes = []

    name_of_reducer_instances = []

    for i in range(num_reducer):
        name_of_reducer_instances.append("reducer" + str(i))

    # print(name_of_reducer_instances)

    for i in range(num_reducer):
        p1 = Process(target=accepting_client_reducer, args=(s, i, operation, ))
        p1.start()
        p2 = Process(target=reducer_spawn, args=(name_of_reducer_instances[i], ))
        p2.start()
        processes.append(p1)
        processes.append(p2)

    for p in processes:
        p.join()

    print("Reducer's job done")

    # Deleting created reducer VMs
    deletion = CloudAPI()
    for instance in name_of_reducer_instances:
        op = deletion.delete_instance(instance)
        deletion.wait_for_operation(op['name'])

    return "task completed"


def destroy_cluster(cid):
    sys.exit()


if __name__ == '__main__':
    start_time = time.time()

    ip_address = Config['A3']['ip_address']
    port = int(Config['A3']['port'])
    cid = init_cluster(ip_address, port)  # Initialise the master node
    operation = Config['A3']['operation']  # WC: Word Count , II: Inverted Index

    file1 = Config['A3']['input_filename1']  # If word count, file1 will be considered
    file2 = Config['A3']['input_filename2']
    num_mapper = int(Config['A3']['n_mapper'])
    num_reducer = int(Config['A3']['n_reducer'])

    run_mapred(file1, file2, num_mapper, num_reducer, operation)
    print("Total time to run:", time.time() - start_time)
    destroy_cluster(cid)
