import logging
import socket
import socketserver
import threading
from abc import ABCMeta, abstractmethod
from commons.handlers.KVDefaultRequestHandler import KVDefaultRequestHandler


class KVNode(socketserver.ThreadingTCPServer, metaclass=ABCMeta):

    # metadata
    _IP = None
    _PORT = None
    _NODE_ID = None

    _registered_data_managers = None
    _logger = None

    def __init__(self, ip='127.0.0.1', port=8700, log_level=logging.DEBUG, request_handler=KVDefaultRequestHandler):

        # initializing the ThreadingTCPServer
        super().__init__(server_address=(ip, port), RequestHandlerClass=request_handler, bind_and_activate=True)

        # preserving metadata
        self._IP = ip
        self._PORT = port
        self._NODE_ID = '{}:{}'.format(self._IP, self._PORT)

        # init logging
        self._init_logging(log_level)
        self._logger.info('starting')

        self.__serve_tcp()

    def _init_logging(self, log_level):

        self._logger = logging.getLogger('{}-{}'.format(self._NODE_ID, self._type()))
        self._logger.setLevel(log_level)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        self._logger.addHandler(ch)

    @abstractmethod
    def _type(self):
        """
        Abstract method to be implemented on each subclass
        :return:
        """

    def __serve_tcp(self):
        # https://docs.python.org/3/library/socketserver.html
        # starting the serving thread (FOREVER)
        # this will subsequently start a new thread per request
        serving_thread = threading.Thread(target=self.serve_forever)
        serving_thread.daemon = True  # exit the serving_thread when the main thread/process terminates
        serving_thread.start()
        self._logger.info('threaded serving running')

    def send_threaded_tcp_request(self, ip, port, msg):

        def send_tcp_request():

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

                cur_thread = threading.current_thread()
                sock.connect((ip, port))

                sock.sendall(bytes(msg, 'ascii'))
                print("{} sent request: {}".format(cur_thread.name, msg))

                response = str(sock.recv(1024), 'ascii')
                print("{} got response: {}".format(cur_thread.name, response))

        request_thread = threading.Thread(target=send_tcp_request)
        request_thread.daemon = True
        request_thread.start()
