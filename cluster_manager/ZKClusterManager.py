from time import sleep
from commons.handlers.KVDefaultRequestHandler import KVDefaultRequestHandler
from commons.nodes.ZKNode import ZKNode
from kazoo.client import ChildrenWatch
from logging import DEBUG


class ZKClusterManager(ZKNode):

    _election = None

    def __init__(self, ip='127.0.0.1', port=8700, zk_hosts='127.0.0.1', zk_root='ultkv', log_level=DEBUG,
                 request_handler=KVDefaultRequestHandler):

        super().__init__(ip=ip, port = port, zk_hosts=zk_hosts, zk_root=zk_root, log_level=log_level,
                         request_handler=request_handler)
        self.register_cluster_manager()

    def register_cluster_manager(self):

        self._logger.info("registering cluster manager")

        # based on http://kazoo.readthedocs.io/en/latest/api/recipe/election.html
        self._election = self._zk_client.Election(self._zk_cluster_managers_path, identifier=self._NODE_ID)
        self._election.run(self.start_cluster_manager)

    def start_cluster_manager(self):

        self._logger.info("starting cluster manager")
        self.init_zk_state()
        self.watch_data_managers_state()

    def init_zk_state(self):

        # initializing ZK state
        zk_state_roots = [
            self._zk_cluster_managers_path,
            self._zk_data_managers_path,
            self._zk_clusters_path
        ]

        for state_root in zk_state_roots:
            if not self._zk_client.exists(state_root):
                self._logger.info('initializing zk state root: {}'.format(state_root))
                self._zk_client.create(state_root, makepath=True)

    def watch_data_managers_state(self):

        self._logger.debug("registering @ChildrenWatch on {}".format(self._zk_data_managers_path))

        @ChildrenWatch(client=self._zk_client, path=self._zk_data_managers_path, send_event=False)
        def state_change(children):

            children = set(children)

            if self._registered_data_managers is None:
                self._logger.info("data managers state found: {}".format(children))
                self._registered_data_managers = set()
            else:
                self._logger.info("data managers state changed from: {} to {}".format(self._registered_data_managers,
                                                                                      children))

            added_data_managers = children - self._registered_data_managers
            removed_data_managers = self._registered_data_managers - children
            self._registered_data_managers = children

            for node_id in added_data_managers:
                [ip, port] = node_id.split(':')
                msg = 'welcome'
                self.send_threaded_tcp_request(ip, int(port), msg)

            for node_id in removed_data_managers:
                print('removed: {}'.format(node_id))

        while True:
            self._logger.debug("cluster manager active; cluster managers state: {}; data managers state: {}".
                               format(self._election.contenders(), self._registered_data_managers))
            sleep(10)

    def _type(self):
        return 'ZKClusterManager'
    #
    #

    def refresh_clusters_state(self):
        pass

    def create_cluster(self, cluster_id, shard_keys, replication):
        pass

if __name__ == '__main__':

    cluster_manager = ZKClusterManager(port=8701)



