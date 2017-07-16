from abc import ABCMeta, abstractmethod
from commons.handlers.KVDefaultRequestHandler import KVDefaultRequestHandler
from commons.nodes.KVNode import KVNode
from kazoo.client import KazooClient
from kazoo.retry import KazooRetry
from logging import DEBUG


class ZKNode(KVNode, metaclass=ABCMeta):

    _zk_hosts = None
    _zk_client = None

    _zk_root = None
    _zk_cluster_managers_path = None
    _zk_data_managers_path = None
    _zk_clusters_path = None

    def __init__(self, ip='127.0.0.1', port=8700, zk_hosts='127.0.0.1', zk_root='ultkv', log_level=DEBUG,
                 request_handler=KVDefaultRequestHandler):

        super().__init__(ip=ip, port=port, log_level=log_level, request_handler=request_handler)
        self.init_zk_client(zk_hosts, zk_root)

    def init_zk_client(self, zk_hosts, zk_root):

        MAX_TRIES = 10
        DELAY = 1.0
        BACKOFF = 2
        MAX_DELAY = 7200
        retry = KazooRetry(max_tries=MAX_TRIES, delay=DELAY, backoff=BACKOFF, max_delay=MAX_DELAY)

        # zk state paths
        self._zk_root = zk_root
        self._zk_cluster_managers_path = '/{}/clusterManagers'.format(self._zk_root)  # also election path
        self._zk_data_managers_path = '/{}/dataManagers'.format(self._zk_root)
        self._zk_clusters_path = '/{}/clusters'.format(self._zk_root)
        ###
        self._zk_hosts = zk_hosts
        self._zk_client = KazooClient(hosts=self._zk_hosts, connection_retry=retry)
        self._zk_client.start()


    @abstractmethod
    def _type(self):
        pass