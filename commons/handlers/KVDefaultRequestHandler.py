import socketserver
import threading


class KVDefaultRequestHandler(socketserver.BaseRequestHandler):

    max_buff_size = 1024

    def handle(self):

        data_in = str(self.request.recv(self.max_buff_size), 'ascii')
        data_out = data_in.upper()

        cur_thread = threading.current_thread()
        print("{} got request: {}".format(cur_thread.name, data_in))

        response = bytes("{}".format(data_in.upper()), 'ascii')
        self.request.sendall(response)
        print("{} sent response: {}".format(cur_thread.name, data_out))
