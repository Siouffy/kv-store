import socket


def request():

    s = socket.socket()
    host = "127.0.0.1"
    port = 8701
    s.connect((host, port))

    while 1:
        str = input ("enter command")
        s.sendall(bytes(str, 'ascii'))
        response = str(s.recv(1024), 'ascii')
        print(response)

if __name__ == "__main__" :
    request()
