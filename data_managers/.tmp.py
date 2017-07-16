# todo: check/update [obselete] implementation

def __generate_shards(self, keys):
    shards = dict()
    for k in keys:
        shards[k] = self._shards[k]
    return shards


def set_master(self, args):
    """
    :param args: String k1,k2,k3,...
    :return:
    """

    keys = args.split(',')
    self.logger.debug("keys are: {}".format(keys))

    for k in keys:
        self._master[k] = True
        self.logger.debug("set {} to MASTER".format(k))

    return {
        'op': 'msr',
        'keys': keys,
        'code': 0
    }


def set_replica(self, args):
    """
    :param args: k1,k2,k3,...
    :return:
    """

    keys = args.split(',')
    self.logger.debug("keys are: {}".format(keys))

    for k in keys:
        self._master[k] = False
        self.logger.debug("set {} to REPLICA".format(k))

    return {
        'op': 'rpl',
        'keys': keys,
        'code': 0
    }


def unpersist(self, args):
    """
    :param args: k1,k2,k3,...
    :return:
    """

    keys = args.split(',')
    self.logger.debug("keys are: {}".format(keys))

    for k in keys:
        self._master[k] = False
        self.logger.debug("set {} to REPLICA".format(k))

        del self._shards[k]
        self._shards[k] = dict()
        self.logger.debug("set {} UNPERSIST".format(k))

    return {
        'op': 'upr',
        'keys': keys,
        'code': 0
    }


def set_data(self, args):
    """
    :param args: String: SET command arguments

            key=value
    """

    arge_delim = '='
    args_elem = args.split(arge_delim)

    key = args_elem[0]
    self.logger.debug("key is: {}".format(key))

    val = arge_delim.join(args_elem[1:])
    self.logger.debug("value is: {}".format(val))

    self.logger.debug("setting {} to {}".format(key, val))
    self._shards[key[0]][key] = val

    return {
        'op': 'set',
        'key': key,
        'value': val,
        'code': 0
    }


def get_data(self, args):
    key = args.strip()
    self.logger.debug("key is: {}".format(key))

    val = self._shards[key[0]][key]
    self.logger.debug("value is: {}".format(val))

    return {
        'op': 'get',
        'key': key,
        'value': val,
        'code': 0
    }


def set_bulk(self, args):
    """
    :param args: string (encoded python dict)

    {
        shard_key: {
            key: value,
            key: value,
            ..
        }

        shard_key: {
            ..
        }

        .
        .

    }
    :return:
    """

    data = json.loads(args)
    self.logger.debug('bulk data loaded .. ')

    for k, v in data.items():
        self._shards[k].update(v)
        self.logger.debug('bulk updated for key: {}'.format(k))

    return {
        'op': 'seb',
        'keys': list(data.keys()),
        'code': 0
    }


def send_bulk(self, args):
    args_delim = 'to'
    args_elem = args.split(args_delim)

    keys = args_elem[0].strip().split(',')
    nodes = args_elem[1].strip().split(',')

    self.logger.debug('send_bulk: {} to {}'.format(keys, nodes))

    shards = self.__generate_shards(keys)
    shards_str = json.dumps(shards)

    request_str = "SEB {}".format(shards_str)
    self.logger.debug('request_str: {}'.format(request_str))

    for node in nodes:
        # connect to the node
        s = socket.socket()
        node_elem = node.split(":")

        host = node_elem[0]
        port = int(node_elem[1])
        s.connect((host, port))

        s.recv(1024)  # initial got you message
        self.logger.debug('connected to node: {}:{}'.format(host, port))

        # send shards to the node via set bulk
        s.send(bytes(request_str, 'utf-8'))

        response_bytes = s.recv(1024)  # assumes res.code is 0 for now
        response = json.loads(response_bytes.decode('utf-8'))
        self.logger.debug('send_bulk response: {}'.format(response))

        # closes connection
        term_str = "TRM"
        s.send(bytes(term_str, 'utf-8'))
        s.close()
        self.logger.debug('connection to {}:{} terminated'.format(host, port))

    return {
        'op': 'snb',
        'code': 0,
        'keys': keys,
        'nodes': nodes
    }


def terminate(self):
    return {
        'op': 'trm',
        'code': 10
    }


def serve_incoming_request(self, request_bytes):
    delim = ' '

    # parse bytes to string
    request = request_bytes.decode('utf-8')
    request_elem = request.split(delim)

    fun = request_elem[0]
    args = delim.join(request_elem[1:])
    self.logger.info("recieved request {}{}{}".format(fun, delim, args))

    if fun == "SET":
        return self.set_data(args)

    elif fun == "GET":
        return self.get_data(args)

    elif fun == "MSR":
        return self.set_master(args)

    elif fun == "RPL":
        return self.set_replica(args)

    elif fun == "UPR":
        return self.unpersist(args)

    elif fun == "SEB":
        return self.set_bulk(args)

    elif fun == "SNB":
        return self.send_bulk(args)

    elif fun == "TRM":
        return self.terminate()

    else:
        return {'code': 1, 'msg': 'unknown request type'}
