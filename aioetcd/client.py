import json
import aiohttp
import asyncio
import aioetcd


class Client:
    def __init__(
            self,
            host='127.0.0.1',
            port=2379,
            protocol='http',
            read_timeout=60,
            allow_redirect=True,
            allow_reconnect=False,
            loop=None,
    ):
        """
        Initialize the client.

        :param host: (optional) If a string, IP to connect to. If a tuple
            ((host, port),(host, port), ...) of known etcd nodes
        :type host: str or tuple
        :param int port: (optional) Port used to connect to etcd.
        :param int read_timeout: (optional) max seconds to wait for a read.
        :param bool allow_redirect: (optional) allow the client to connect to
            other nodes.
        :param str protocol:  (optional) Protocol used to connect to etcd.
        :param bool allow_reconnect: (optional) allow the client to reconnect
            to another etcd server in the cluster in the case the default one
            does not respond.
        :param loop: event loop to use internaly. If None, use
            asyncio.get_event_loop()
        """
        self.loop = loop if loop is not None else asyncio.get_event_loop()

        def uri(host, port):
            return '%s://%s:%d' % (protocol, host, port)

        if isinstance(host, tuple):
            self._machines_cache = [uri(*conn) for conn in host]
            self._base_uri = self._machines_cache[0]
        else:
            self._base_uri = uri(host, port)
            self._machines_cache = [self._base_uri]

        self.read_timeout = read_timeout
        self.allow_redirect = allow_redirect
        self.allow_reconnect = allow_reconnect
        self._conn = aiohttp.TCPConnector()
        self._cache_update_scheduled = True
        self.loop.create_task(self._update_machine_cache())

    # # high level operations

    async def get(self, key):
        """
        Returns the value of the key 'key'. Use Client.read for more
        control and to get a full detailed aioetcd.EtcdResult

        :param str key:  Key.
        :returns: str value of the key
        :raises: KeyError if the key doesn't exists.
        """
        res = await self.read(key)
        return res

    async def get_value(self, key):
        res = await self.read(key)
        if res.value is None:
            raise aioetcd.EtcdException(102)
        return res.value

    async def set(self, key, value, ttl=None):
        """
        set the value of the key :param key: to the value :param value:. Is an
        alias of :method write: to expose a get/set API (with only most common
        args)

        """
        result = await self._write(key, value, ttl=ttl)
        return result

    async def delete(self, key, recursive=None, dir=None, **params):
        """
        Removed a key from etcd.
        :param str key:  Key.
        :param bool recursive: if we want to recursively delete a directory,
            set it to true
        :param bool dir: if we want to delete a directory, set it to true
        :param str prevValue: compare key to this value, and delete only if
            corresponding (optional).
        :param int prevIndex: modify key only if actual modifiedIndex matches
            the provided one (optional).

        :returns: aioetcd.EtcdResult

        :raises: KeyValue: if the key doesn't exists.
        """
        key = key.lstrip('/')

        if recursive is not None:
            params['recursive'] = recursive and "true" or "false"
        if dir is not None:
            params['dir'] = dir and "true" or "false"

        response = await self._delete("/v2/keys/%s" % key, params=params)
        return self._result_from_response(response)

    async def machines(self):
        resp = await self._get("/v2/machines")
        raw = await resp.text()
        return [m.strip() for m in raw.split(',')]

    async def leader(self):
        """
        Returns:
            str. the leader of the cluster.
        """
        resp = await self._get("/v2/stats/leader")
        raw = await resp.json()
        return raw["leader"]

    async def watch(self, key, index=None, timeout=None):
        """
        Blocks until a new event has been received, starting at index 'index'
        :param str key:  key to watch
        :param int index: Index to start from. if None, start from current
            index
        :param timeout:  max seconds to wait for a read. If None, no timeout,
            blocks forever
        :type timeout: int or float

        :returns: client.EtcdResult

        :raises KeyValue:  If the key doesn't exists.
        :raises asyncio.TimeoutError: If timeout is reached.

        """
        params = dict(wait=True)
        if index is not None:
            params['waitIndex'] = index
        if timeout is not None:
            params['timeout'] = timeout
        while 42:
            try:
                response = await self.read(key, None, **params)
                return response
            except asyncio.TimeoutError:
                if timeout is not None:
                    raise

    async def watch_iterator(self, key, index=None):
        """
        return an iterator of self.watch() coroutines

        :param str key:  key to watch
        :param int index: (optional) Index to start from. if None, start from
            current index
        :raises KeyValue:  If the key doesn't exists.

        Usage::

            >>> wi = yield from client.watch_iterator(key)
            >>> for watcher in wi:
            >>>     resp = yield from watcher
            >>>     print(resp.value)
            >>>     if resp.value == '42':
            >>>         break

        """
        # TODO: add a timeout that raises StopIteration
        if index is None:
            result = await self.read(key)
            index = result.modifiedIndex
        return iter(self._watch_forever(key, index))

    def _watch_forever(self, key, index=None, timeout=None):
        while 42:
            index += 1
            yield self.watch(key, index)

    async def mkdir(self, key, ttl=None):
        """ create a dir key with the given ttl"""
        resp = await self._write(key, None, dir=True, ttl=None)
        return resp

    def read_sync(self, key, **params):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(self.read(key, loop, **params))

    async def _read_headers(self, key, loop=None, **params):
        loop = loop if loop is not None else self.loop
        key = key.lstrip('/')
        timeout = params.pop('timeout', self.read_timeout)
        for (k, v) in params.items():
            if type(v) == bool:
                params[k] = v and "true" or "false"
            else:
                params[k] = v
        response = await self._get(
            "/v2/keys/%s" % key, params=params, timeout=timeout, loop=loop
        )
        return response

    async def _decode_response(self, response, timeout=None, loop=None):
        if timeout is None:
            timeout = self.read_timeout
        data = await asyncio.wait_for(response.text(), timeout, loop=loop)
        try:
            res = json.loads(data)
            headers = response.headers
            res['etcd_index'] = int(headers.get('x-etcd-index', 1))
            res['raft_index'] = int(headers.get('x-raft-index', 1))
            res['new'] = response.status == 201
            res['client'] = self
            return res
        except Exception as e:
            raise aioetcd.EtcdException(
                'Unable to decode server response: %s\n\nData was: %s' %
                (e, data))

    async def read(self, key, loop=None, **params):
        """
        Returns the value of the key 'key'.

        :param str key:  Key.
        :param loop: the event loop to use for this request. Used internally
            for sync_read
        :param bool recursive (bool): If you should fetch recursively a dir
        :param bool wait (bool): If we should wait and return next time the
            key is changed
        :param int waitIndex (int): The index to fetch results from.
        :param bool sorted (bool): Sort the output keys (alphanumerically)
        :param int timeout (int):  max seconds to wait for a read.

        :returns:
            aioetcd.EtcdResult (or an array of client.EtcdResult if a
            subtree is queried)

        :raises:
            KeyValue:  If the key doesn't exists.
            asyncio.TimeoutError
        """
        loop = loop if loop is not None else self.loop
        timeout = params.get('timeout', self.read_timeout)
        head = await self._read_headers(key, loop, **params)
        # TODO: substract current time from timeout
        result = await self._result_from_response(head, timeout, loop)
        return result

    async def _write(self, key, value, append=False, **params):
        """
        Writes the value for a key, possibly doing atomit Compare-and-Swap

        Args:
        :param str key:  Key.
        :param str value:  value to set
        :param int ttl:  Time in seconds of expiration (optional).
        :param bool dir: Set to true if we are writing a directory; default is
            false.
        :param bool append: If true, it will post to append the new value to
            the dir, creating a sequential key. Defaults to false.
        :param str prevValue: compare key to this value, and swap only if
            corresponding (optional).
        :param int prevIndex: modify key only if actual modifiedIndex matches
            the provided one (optional).
        :param bool prevExist: If false, only create key; if true, only update
            key.

        Returns:
            client.EtcdResult
        """
        key = key.lstrip('/')
        # params = {}
        if value is not None:
            params['value'] = value
        if params.get('dir', False):
            if value:
                raise aioetcd.EtcdException(
                    'Cannot create a directory with a value')
        for (k, v) in list(params.items()):
            if type(v) == bool:
                params[k] = v and "true" or "false"
            elif v is None:
                del params[k]

        method = append and self._post or self._put
        path = "/v2/keys/%s" % key
        response = await method(path, params=params)
        result = await self._result_from_response(response)
        return result

    async def _result_from_response(self, response, timeout=None, loop=None):
        """ Creates an EtcdResult from json dictionary """
        params = await self._decode_response(response, timeout, loop)
        if 'errorCode' in params:
            raise aioetcd.EtcdException(params['errorCode'])
        return aioetcd.Node(**params)

    async def _get(self, path, params=None, timeout=None, loop=None):
        resp = await self._execute('get', path, params, timeout, loop)
        return resp

    async def _put(self, path, params=None, timeout=None):
        resp = await self._execute('put', path, params, timeout)
        return resp

    async def _post(self, path, params=None, timeout=None):
        resp = await self._execute('post', path, params, timeout)
        return resp

    async def _delete(self, path, params=None, timeout=None):
        resp = await self._execute('delete', path, params, timeout)
        return resp

    async def _update_machine_cache(self):
        if self.allow_reconnect:
            self._machine_cache = await self.machines()
        self._cache_update_scheduled = False

    async def _execute(self, method, path, params=None, timeout=None, loop=None):
        if loop is None:
            loop = self.loop
        if timeout is None:
            timeout = self.read_timeout
        failed = False
        # TODO: whatif self._machines_cache is empty ?
        for idx, uri in enumerate(self._machines_cache):
            try:
                resp = await asyncio.wait_for(
                    aiohttp.request(
                        method, uri + path, params=params, loop=loop
                    ),
                    timeout, loop=loop
                )
                if failed:
                    self._machine_cache = self._machine_cache[idx:]
                    if not self._cache_update_scheduled:
                        self._cache_update_scheduled = True
                        self.loop.create_task(self._update_machine_cache())
                return resp
            except asyncio.TimeoutError:
                failed = True
        raise asyncio.TimeoutError()
