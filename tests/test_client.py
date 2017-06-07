import asyncio
import pytest
from aioetcd.client import Client
from aioetcd import FileNode


class EtcdError(OSError):
    def __init__(self, retcode, stderr):
        OSError.__init__(self, stderr)
        self.retcode = retcode


class EtcdController:
    def __init__(self, loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()

    async def _shell_exec(self, cmd):
        shell = asyncio.create_subprocess_shell(
            cmd, stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
        )
        proc = await shell
        out, err = await proc.communicate()
        retcode = await proc.wait()
        if retcode:
            raise EtcdError(retcode, err.decode('ascii'))
        return out.decode('ascii').rstrip()

    async def set(self, key, value):
        await self._shell_exec('etcdctl set {} {}'.format(key, value))

    async def mkdir(self, key):
        await self._shell_exec('etcdctl mkdir {}'.format(key))

    async def get(self, key):
        val = await self._shell_exec('etcdctl get {}'.format(key))
        return val

    async def rm(self, key):
        await self._shell_exec('etcdctl rm {}'.format(key))

    async def rmdir(self, key):
        await self._shell_exec('etcdctl rmdir {}'.format(key))

    async def leader(self):
        resp = await self._shell_exec('etcdctl member list')
        for line in resp.split("\n"):
            if "isLeader=true" in line:
                return line.split(" ", 1)[0][:-1]


@pytest.fixture
def client(event_loop):
    return Client(loop=event_loop)


@pytest.fixture
def ctl(event_loop):
    return EtcdController(loop=event_loop)


@pytest.mark.asyncio
async def test_leader(client, ctl):
    leader = await client.leader()
    cmd_leader = await ctl.leader()
    assert leader == cmd_leader


@pytest.mark.asyncio
async def test_machines(client):
    machines = await client.machines()
    assert machines, ["http://localhost:2379"]


@pytest.mark.asyncio
async def test_get(client, ctl):
    await ctl.set('test1', 43)
    node = await client.get('test1')
    assert node.value == '43'
    await ctl.rm("/test1")


@pytest.mark.asyncio
async def test_get_value(client, ctl):
    await ctl.set('test1', 42)
    val = await client.get_value('test1')
    assert val == '42'
    await ctl.rm("/test1")


@pytest.mark.asyncio
async def test_set(client, ctl):
    await ctl.set('test1', 43)
    node = await client.set('test1', 44)
    assert isinstance(node, FileNode)
    val = await ctl.get('test1')
    assert val == '44'
    await ctl.rm("/test1")


@pytest.mark.asyncio
async def test_mkdir(client, ctl):
    dirname = '/testdir'
    dir_ = await client.mkdir(dirname)
    await ctl.set(dirname + '/file', 42)
    assert dir_.key == dirname
    assert dir_.ttl == None
    assert dir_.value == None
    assert dir_.prev_node == None
    assert dir_._children == []
    await ctl.rm("/testdir/file")
    await ctl.rmdir("/testdir")


@pytest.mark.asyncio
async def test_delete(client, ctl):
    await ctl.set('test1', 42)
    val = await client.get_value('test1')
    assert val == '42'
    await client.delete('test1')
    with pytest.raises(EtcdError, match=r'^Error:\s+100:'):
        await ctl.get('test1')


@pytest.mark.asyncio(timeout=10)
async def test_watch(event_loop, client, ctl):
    key = 'test_watch'
    await ctl.set(key, 42)

    watch_fut = asyncio.Future(loop=event_loop)
    watching_fut = asyncio.Future(loop=event_loop)

    async def watcher(key):
        watching_fut.set_result(True)
        await client.watch(key)
        watch_fut.set_result(True)

    asyncio.ensure_future(watcher(key), loop=event_loop)

    await watching_fut

    for i in range(42):
        await asyncio.sleep(0.02)
        assert not watch_fut.done()

    await ctl.set(key, 42)
    assert await watch_fut
    await ctl.rm(key)


@pytest.mark.asyncio
async def test_watch_index(event_loop, client, ctl):
    key = 'test_watch'
    await ctl.set(key, 42)

    # test a single key
    watch_fut = asyncio.Future()

    async def watcher(key, index=None, timeout=None):
        await client.watch(key, index, timeout)
        watch_fut.set_result(True)

    # test with a wait index
    node = await client.get(key)
    event_loop.create_task(watcher(key, node.modifiedIndex + 2))
    await asyncio.sleep(0.001)
    await ctl.set(key, 42)
    await asyncio.sleep(0.001)
    # index is not reached yet, it should not trigger
    assert not watch_fut.done()
    await ctl.set(key, 42)

    assert await watch_fut
    await ctl.rm("/test_watch")


@pytest.mark.asyncio
async def test_watch_timeout(event_loop, client, ctl):
    key = 'test_watch'
    await ctl.set(key, 42)

    # test a single key
    watch_fut = asyncio.Future()

    async def watcher(key, index=None, timeout=None):
        await client.watch(key, index, timeout)
        watch_fut.set_result(True)

    # test timeout
    import time
    start = time.time()
    with pytest.raises(asyncio.TimeoutError):
        await watcher(key, timeout=0.2)
    assert int((time.time() - start) * 10) == 2
    assert not watch_fut.done()
    await ctl.rm("/test_watch")


@pytest.mark.asyncio
async def test_watch_iterator(event_loop, client, ctl):
    key = 'test_iterwatch'
    await ctl.set(key, 42)

    # test a single key
    async_results = False

    async def iterator_watcher(key):
        async_results = []
        it = await client.watch_iterator(key)
        for w in it:
            resp = await w
            async_results.append(resp.value)
            if resp.value == '3':
                break

    event_loop.create_task(iterator_watcher(key))

    await asyncio.sleep(0.1)

    for i in range(10):
        await ctl.set(key, i)

    await asyncio.sleep(0.1)

    assert async_results == ['0', '1', '2', '3']
    await ctl.rm("/test_iterwatch")


@pytest.mark.asyncio
async def test_updatedir(client, ctl):
    dirname = '/testdir2'
    await ctl.mkdir(dirname)
    await client.get(dirname)
    await ctl.rmdir("/testdir2")


@pytest.mark.asyncio
async def test_changed(client, ctl):
    key = 'test_changed'
    await ctl.set(key, "orig")
    node = await client.get(key)
    changed = await node.changed()

    assert not changed

    val = await ctl.set(key, 'changed')
    await asyncio.sleep(0.01)
    changed = await node.changed()

    assert changed

    val = await client.get_value(key)

    assert val == "changed"

    await ctl.rm("/test_changed")


@pytest.mark.asyncio
async def test_update(client, ctl):
    key = 'test_update'
    await ctl.set(key, "orig")
    node = await client.get(key)
    await ctl.set(key, 'changed')
    await node.update()

    assert node.value == "changed"

    await ctl.rm("/test_update")


@pytest.mark.asyncio
async def test_prev_node(client, ctl):
    key = 'test_prev'
    await client.set(key, "orig")
    node2 = await client.set(key, "changed")

    assert node2.value == "changed"

    await ctl.rm("/test_prev")
