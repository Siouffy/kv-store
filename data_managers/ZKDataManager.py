from time import sleep

from commons.handlers.KVDefaultRequestHandler import KVDefaultRequestHandler
from commons.nodes.ZKNode import ZKNode
from kazoo.client import ChildrenWatch
from logging import DEBUG


class ZKDataManager(ZKNode):

    _master = dict()  # dict of boolean representing if the node is a Master for a shard / key
    _shards = dict()

    def __init__(self, ip='127.0.0.1', port=8800, zk_hosts='127.0.0.1', zk_root='ultkv', log_level=DEBUG,
                 request_handler=KVDefaultRequestHandler):

        super().__init__(ip=ip, port=port, zk_hosts=zk_hosts, zk_root=zk_root, log_level=log_level,
                         request_handler=request_handler)
        self.register_data_manager()
        self.start_data_manager()

        # initializing node metadata
        #self.logger.debug('initializing _master')
        #for k in self._KEYS[key]['literals']:
        #    self._master[k] = False

        # initializing shards
        #self.logger.debug('initializing shards')
        #for k in self._KEYS[key]['literals']:
        #    self._shards[k] = dict()
        # self.serve()

    def _type(self):
        return 'ZKDataManager'

    def register_data_manager(self):

        zk_path = '{}/{}'.format(self._zk_data_managers_path, self._NODE_ID)
        self._zk_client.create(path=zk_path, ephemeral=True)
        self._logger.info('registered data manager to: {}'.format(zk_path))

    def start_data_manager(self):

        self._logger.info("starting data manager")
        self.watch_data_managers_state()

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
                if node_id != self._NODE_ID:
                    [ip, port] = node_id.split(':')
                    msg = 'welcome'
                    self.send_threaded_tcp_request(ip, int(port), msg)

            for node_id in removed_data_managers:
                print('removed: {}'.format(node_id))

        while True:
            self._logger.debug("data manager active; data managers state: {}".format(self._registered_data_managers))
            sleep(10)








if __name__ == "__main__":
    ZKDataManager()
