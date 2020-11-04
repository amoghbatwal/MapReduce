import socket


class Operation:
    def connect(self, serverip):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((serverip, 8080))

    def command(self, input_from_client):
        try:
            self.s.send(input_from_client.encode("utf-8"))
            x = self.s.recv(50000)  # receiving the output
            # print("X:", x)
            return x.decode()
        except IOError:
            print("IOError")

    def get_all(self, key):
        msg = 'GA ' + str(key) + "\r\n"
        self.s.send(msg.encode("utf-8"))
        x = self.s.recv(100000000).decode("utf-8")
        return x

    def disconnect(self):
        msg = 'DISCONNECT'
        self.s.send(msg.encode('utf-8'))
        self.s.close()
        print("Connection closed by Client\r\n")
