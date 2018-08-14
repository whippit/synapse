'''
This contains the core test helper code used in Synapse.

This gives the opportunity for third-party users of Synapse to test their
code using some of the same of the same helpers used to test Synapse.

The core class, synapse.tests.utils.SynTest is a subclass of unittest.TestCase,
with several wrapper functions to allow for easier calls to assert* functions,
with less typing.  There are also Synapse specific helpers, to load Cortexes and
whole both multi-component environments into memory.

Since SynTest is built from unittest.TestCase, the use of SynTest is
compatible with the unittest, nose and pytest frameworks.  This does not lock
users into a particular test framework; while at the same time allowing base
use to be invoked via the built-in Unittest library.
'''
import io
import os
import sys
import types
import shutil
import logging
import pathlib
import tempfile
import unittest
import threading
import contextlib

import synapse.exc as s_exc
import synapse.data as s_data
import synapse.glob as s_glob
import synapse.cells as s_cells
import synapse.common as s_common
import synapse.cortex as s_cortex
import synapse.daemon as s_daemon
import synapse.lib.const as s_const
import synapse.lib.scope as s_scope
import synapse.lib.types as s_types
import synapse.eventbus as s_eventbus
import synapse.telepath as s_telepath
import synapse.lib.module as s_module
import synapse.lib.output as s_output
import synapse.lib.certdir as s_certdir
import synapse.lib.thishost as s_thishost

logger = logging.getLogger(__name__)

# Default LMDB map size for tests
TEST_MAP_SIZE = s_const.gibibyte

def writeCerts(dirn):
    '''
    Copy test SSL certs from synapse.data to a directory.

    Args:
        dirn (str): Path to write files too.

    Notes:
        Writes the following files to disk:
        . ca.crt
        . ca.key
        . ca.pem
        . server.crt
        . server.key
        . server.pem
        . root.crt
        . root.key
        . user.crt
        . user.key

        The ca has signed all three certs.  The ``server.crt`` is for
        a server running on localhost. The ``root.crt`` and ``user.crt``
        certs are both are user certs which can connect. They have the
        common names "root@localhost" and "user@localhost", respectively.

    Returns:
        None
    '''
    fns = ('ca.crt', 'ca.key', 'ca.pem',
           'server.crt', 'server.key', 'server.pem',
           'root.crt', 'root.key', 'user.crt', 'user.key')
    for fn in fns:
        byts = s_data.get(fn)
        dst = os.path.join(dirn, fn)
        if not os.path.exists(dst):
            with s_common.genfile(dst) as fd:
                fd.write(byts)

class TestType(s_types.Type):

    def postTypeInit(self):
        self.setNormFunc(str, self._normPyStr)

    def _normPyStr(self, valu):
        return valu.lower(), {}

    def indx(self, norm):
        return norm.encode('utf8')

class ThreeType(s_types.Type):

    def norm(self, valu):
        return 3, {'subs': {'three': 3}}

    def repr(self, valu):
        return '3'

    def indx(self, norm):
        return '3'.encode('utf8')

testmodel = {

    'ctors': (
        ('testtype', 'synapse.tests.utils.TestType', {}, {}),
        ('testthreetype', 'synapse.tests.utils.ThreeType', {}, {}),
    ),

    'types': (
        ('testtype10', ('testtype', {'foo': 10}), {
            'doc': 'A fake type.'}),

        ('testlower', ('str', {'lower': True}), {}),

        ('testtime', ('time', {}), {}),

        ('testint', ('int', {}), {}),
        ('teststr', ('str', {}), {}),
        ('testauto', ('str', {}), {}),
        ('testguid', ('guid', {}), {}),

        ('testcomp', ('comp', {'fields': (
            ('hehe', 'testint'),
            ('haha', 'testlower'))
        }), {'doc': 'A fake comp type.'}),
        ('testcomplexcomp', ('comp', {'fields': (
            ('foo', 'testint'),
            ('bar', ('str', {'lower': True}),),
        )}), {'doc': 'A complex comp type.'}),
        ('testhexa', ('hex', {}), {'doc': 'anysize test hex type'}),
        ('testhex4', ('hex', {'size': 4}), {'doc': 'size 4 test hex type'}),

        ('pivtarg', ('str', {}), {}),
        ('pivcomp', ('comp', {'fields': (('targ', 'pivtarg'), ('lulz', 'teststr'))}), {}),

        ('cycle0', ('str', {}), {}),
        ('cycle1', ('str', {}), {}),
    ),

    'forms': (

        ('testtype10', {}, (

            ('intprop', ('int', {'min': 20, 'max': 30}), {
                'defval': 20}),

            ('strprop', ('str', {'lower': 1}), {
                'defval': 'asdf'}),

            ('guidprop', ('guid', {'lower': 1}), {
                'defval': '*'}),

            ('locprop', ('loc', {}), {
                'defval': '??'}),
        )),

        ('cycle0', {}, (
            ('cycle1', ('cycle1', {}), {}),
        )),

        ('cycle1', {}, (
            ('cycle0', ('cycle0', {}), {}),
        )),

        ('testcomp', {}, (
            ('hehe', ('testint', {}), {'ro': 1}),
            ('haha', ('testlower', {}), {'ro': 1}),
        )),

        ('testcomplexcomp', {}, (
            ('foo', ('testint', {}), {'ro': 1}),
            ('bar', ('str', {'lower': 1}), {'ro': 1})
        )),

        ('testint', {}, ()),
        ('testguid', {}, (
            ('tick', ('testtime', {}), {}),
        )),

        ('teststr', {}, (
            ('bar', ('ndef', {}), {}),
            ('baz', ('nodeprop', {}), {}),
            ('tick', ('testtime', {}), {}),
        )),

        ('testthreetype', {}, (
            ('three', ('int', {}), {}),
        )),
        ('testauto', {}, ()),
        ('testhexa', {}, ()),
        ('testhex4', {}, ()),

        ('pivtarg', {}, (
            ('name', ('str', {}), {}),
        )),

        ('pivcomp', {}, (
            ('targ', ('pivtarg', {}), {}),
            ('lulz', ('teststr', {}), {}),
            ('tick', ('time', {}), {}),
        )),
    ),
}

class TestModule(s_module.CoreModule):

    def initCoreModule(self):
        self.core.setFeedFunc('com.test.record', self.addTestRecords)

    def addTestRecords(self, snap, items):
        for name in items:
            snap.addNode('teststr', name)

    def getModelDefs(self):
        return (
            ('test', testmodel),
        )

class TstEnv:

    def __init__(self):
        self.items = {}
        self.tofini = []

    def __getattr__(self, prop):
        item = self.items.get(prop)
        if item is None:
            raise AttributeError(prop)
        return item

    def __enter__(self):
        return self

    def __exit__(self, cls, exc, tb):
        self.fini()

    def add(self, name, item, fini=False):
        self.items[name] = item
        if fini:
            self.tofini.append(item)

    def fini(self):
        for bus in self.tofini:
            bus.fini()

class TstOutPut(s_output.OutPutStr):

    def expect(self, substr, throw=True):
        '''
        Check if a string is present in the messages captured by the OutPutStr object.

        Args:
            substr (str): String to check for the existence of.
            throw (bool): If True, a missing substr results in a Exception being thrown.

        Returns:
            bool: True if the string is present; False if the string is not present and throw is False.
        '''
        outs = str(self)
        if outs.find(substr) == -1:
            if throw:
                raise Exception('TestOutPut.expect(%s) not in %s' % (substr, outs))
            return False
        return True

class TestSteps:
    '''
    A class to assist with interlocking for multi-thread tests.

    Args:
        names (list): A list of names of tests steps as strings.
    '''
    def __init__(self, names):
        self.steps = {}
        self.names = names

        for name in names:
            self.steps[name] = threading.Event()

    def done(self, step):
        '''
        Mark the step name as complete.

        Args:
            step (str): The step name to mark complete
        '''
        self.steps[step].set()

    def wait(self, step, timeout=None):
        '''
        Wait (up to timeout seconds) for a step to complete.

        Args:
            step (str): The step name to wait for.
            timeout (int): The timeout in seconds (or None)

        Returns:
            bool: True if the step is completed within the wait timeout.

        Raises:
            StepTimeout: on wait timeout
        '''
        if not self.steps[step].wait(timeout=timeout):
            raise s_exc.StepTimeout(mesg='timeout waiting for step', step=step)
        return True

    def step(self, done, wait, timeout=None):
        '''
        Complete a step and wait for another.

        Args:
            done (str): The step name to complete.
            wait (str): The step name to wait for.
            timeout (int): The wait timeout.
        '''
        self.done(done)
        return self.wait(wait, timeout=timeout)

    def waitall(self, timeout=None):
        '''
        Wait for all the steps to be complete.

        Args:
            timeout (int): The wait timeout (per step).

        Returns:
            bool: True when all steps have completed within the alloted time.

        Raises:
            StepTimeout: When the first step fails to complete in the given time.
        '''
        for name in self.names:
            self.wait(name, timeout=timeout)
        return True

    def clear(self, step):
        '''
        Clear the event for a given step.

        Args:
            step (str): The name of the step.
        '''
        self.steps[step].clear()

class CmdGenerator(s_eventbus.EventBus):
    '''
    Generates a callable object which can be used with unittest.mock.patch in
    order to do CLI driven testing.

    Args:
        cmds (list): List of commands to send to callers.
        on_end (str, Exception): Either a string or a exception class that is
        respectively returned or raised when all the provided commands have been consumed.

    Examples:
        Use the CmdGenerator to issue a series of commands to a Cli object during a test::

            outp = self.getTestOutp()  # self is a SynTest instance
            cmdg = CmdGenerator(['help', 'ask hehe:haha=1234', 'quit'])
            # Patch the get_input command to call our CmdGenerator instance
            with mock.patch('synapse.lib.cli.get_input', cmdg) as p:
                with s_cli.Cli(None, outp) as cli:
                    cli.runCmdLoop()
                    self.eq(cli.isfini, True)

    Notes:
        This EventBus reacts to the event ``syn:cmdg:add`` to add additional
        command strings after initialization. The value of the ``cmd`` argument
        is appended to the list of commands returned by the CmdGenerator.
    '''

    def __init__(self, cmds, on_end='quit'):
        s_eventbus.EventBus.__init__(self)
        self.cmds = list(cmds)
        self.cur_command = 0
        self.end_action = on_end

        self.on('syn:cmdg:add', self._onCmdAdd)

    def _onCmdAdd(self, mesg):
        cmd = mesg[1].get('cmd')
        self.addCmd(cmd)

    def addCmd(self, cmd):
        '''
        Add a command to the end of the list of commands returned by the CmdGenerator.

        Args:
            cmd (str): Command to add to the list of commands to return.
        '''
        self.cmds.append(cmd)

    def __call__(self, *args, **kwargs):
        try:
            ret = self.cmds[self.cur_command]
        except IndexError:
            ret = self._on_end()
            return ret
        else:
            self.cur_command = self.cur_command + 1
            return ret

    def _on_end(self):
        if isinstance(self.end_action, str):
            return self.end_action
        if callable(self.end_action) and issubclass(self.end_action, BaseException):
            raise self.end_action('No further actions')
        raise Exception('Unhandled end action')

class StreamEvent(io.StringIO, threading.Event):
    '''
    A combination of a io.StringIO object and a threading.Event object.
    '''
    def __init__(self, *args, **kwargs):
        io.StringIO.__init__(self, *args, **kwargs)
        threading.Event.__init__(self)
        self.mesg = ''

    def setMesg(self, mesg):
        '''
        Clear the internal event and set a new message that is used to set the event.

        Args:
            mesg (str): The string to monitor for.

        Returns:
            None
        '''
        self.mesg = mesg
        self.clear()

    def write(self, s):
        io.StringIO.write(self, s)
        if self.mesg and self.mesg in s:
            self.set()

class SynTest(unittest.TestCase):

    def setUp(self):
        self.alt_write_layer = None

    def checkNode(self, node, expected):
        ex_ndef, ex_props = expected
        self.eq(node.ndef, ex_ndef)
        [self.eq(node.get(k), v, msg=f'Prop {k} does not match') for (k, v) in ex_props.items()]

        diff = {prop for prop in (set(node.props) - set(ex_props)) if not prop.startswith('.')}
        if diff:
            logger.warning('form(%s): untested properties: %s', node.form.name, diff)

    def getTestWait(self, bus, size, *evts):
        return s_eventbus.Waiter(bus, size, *evts)

    def printed(self, msgs, text):
        # a helper for testing storm print message output
        for mesg in msgs:
            if mesg[0] == 'print':
                if mesg[1].get('mesg') == text:
                    return

        raise Exception('print output not found: %r' % (text,))

    def getTestSteps(self, names):
        '''
        Return a TestSteps instance for the given step names.

        Args:
            names ([str]): The list of step names.
        '''
        return TestSteps(names)

    def skip(self, mesg):
        raise unittest.SkipTest(mesg)

    def skipIfNoInternet(self):  # pragma: no cover
        '''
        Allow skipping a test if SYN_TEST_SKIP_INTERNET envar is set.

        Raises:
            unittest.SkipTest if SYN_TEST_SKIP_INTERNET envar is set to a integer greater than 1.
        '''
        if bool(int(os.getenv('SYN_TEST_SKIP_INTERNET', 0))):
            raise unittest.SkipTest('SYN_TEST_SKIP_INTERNET envar set')

    def skipLongTest(self):  # pragma: no cover
        '''
        Allow skipping a test if SYN_TEST_SKIP_LONG envar is set.

        Raises:
            unittest.SkipTest if SYN_TEST_SKIP_LONG envar is set to a integer greater than 1.
        '''
        if bool(int(os.getenv('SYN_TEST_SKIP_LONG', 0))):
            raise unittest.SkipTest('SYN_TEST_SKIP_LONG envar set')

    def getTestOutp(self):
        '''
        Get a Output instance with a expects() function.

        Returns:
            TstOutPut: A TstOutPut instance.
        '''
        return TstOutPut()

    def thisHostMust(self, **props):  # pragma: no cover
        '''
        Requires a host having a specific property.

        Args:
            **props:

        Raises:
            unittest.SkipTest if the required property is missing.
        '''
        for k, v in props.items():
            if s_thishost.get(k) != v:
                raise unittest.SkipTest('skip thishost: %s!=%r' % (k, v))

    def thisHostMustNot(self, **props):  # pragma: no cover
        '''
        Requires a host to not have a specific property.

        Args:
            **props:

        Raises:
            unittest.SkipTest if the required property is missing.
        '''

        for k, v in props.items():
            if s_thishost.get(k) == v:
                raise unittest.SkipTest('skip thishost: %s==%r' % (k, v))

    @contextlib.contextmanager
    def getTestCore(self, mirror='testcore', conf=None, extra_layers=None):
        '''
        Return a simple test Cortex.

        Args:
           conf:  additional configuration entries.  Combined with contents from mirror.
        '''
        with self.getTestDir(mirror=mirror) as dirn:
            s_cells.deploy('cortex', dirn)
            s_common.yamlmod(conf, dirn, 'cell.yaml')
            ldir = s_common.gendir(dirn, 'layers')
            layerdir = pathlib.Path(ldir, '000-default')
            if self.alt_write_layer:
                os.symlink(self.alt_write_layer, layerdir)
            else:
                layerdir.mkdir()
                s_cells.deploy('layer-lmdb', layerdir)
                s_common.yamlmod({'lmdb:mapsize': TEST_MAP_SIZE}, layerdir, 'cell.yaml')
            for i, fn in enumerate(extra_layers or []):
                src = pathlib.Path(fn).resolve()
                os.symlink(src, pathlib.Path(ldir, f'{i + 1:03}-testlayer'))

            with s_cortex.Cortex(dirn) as core:
                yield core

    @contextlib.contextmanager
    def getTestDmon(self, mirror='dmontest'):

        with self.getTestDir(mirror=mirror) as dirn:
            coredir = pathlib.Path(dirn, 'cells', 'core')
            if coredir.is_dir():
                ldir = s_common.gendir(coredir, 'layers')
                if self.alt_write_layer:
                    os.symlink(self.alt_write_layer, pathlib.Path(ldir, '000-default'))

            certdir = s_certdir.defdir

            with s_daemon.Daemon(dirn) as dmon:

                # act like synapse.tools.dmon...
                s_certdir.defdir = s_common.genpath(dirn, 'certs')

                yield dmon

                s_certdir.defdir = certdir

    @contextlib.contextmanager
    def getTestDir(self, mirror=None):
        '''
        Get a temporary directory for test purposes.
        This destroys the directory afterwards.

        Args:
            mirror (str): A directory to mirror into the test directory.

        Notes:
            If the ``mirror`` argument is a directory, that directory will be
            copied to the test directory. If it is not a directory, the helper
            ``getTestFilePath`` is used to get the test directory under the
            ``synapse/tests/files/`` directory.

        Returns:
            str: The path to a temporary directory.
        '''
        tempdir = tempfile.mkdtemp()

        try:

            if mirror is not None:
                if os.path.isdir(mirror):
                    srcpath = mirror
                else:
                    srcpath = self.getTestFilePath(mirror)
                dstpath = os.path.join(tempdir, 'mirror')
                shutil.copytree(srcpath, dstpath)
                s_scope.set('dirn', dstpath)
                yield dstpath

            else:
                s_scope.set('dirn', tempdir)
                yield tempdir

        finally:
            s_scope.pop('dirn')
            shutil.rmtree(tempdir, ignore_errors=True)

    def getTestFilePath(self, *names):
        import synapse.tests.common
        path = os.path.dirname(synapse.tests.common.__file__)
        return os.path.join(path, 'files', *names)

    @contextlib.contextmanager
    def getLoggerStream(self, logname, mesg=''):
        '''
        Get a logger and attach a io.StringIO object to the logger to capture log messages.

        Args:
            logname (str): Name of the logger to get.
            mesg (str): A string which, if provided, sets the StreamEvent event if a message
            containing the string is written to the log.

        Examples:
            Do an action and get the stream of log messages to check against::

                with self.getLoggerStream('synapse.foo.bar') as stream:
                    # Do something that triggers a log message
                    doSomthing()

                stream.seek(0)
                mesgs = stream.read()
                # Do something with messages

            Do an action and wait for a specific log message to be written::

                with self.getLoggerStream('synapse.foo.bar', 'big badda boom happened') as stream:
                    # Do something that triggers a log message
                    doSomthing()
                    stream.wait(timeout=10)  # Wait for the mesg to be written to the stream

                stream.seek(0)
                mesgs = stream.read()
                # Do something with messages

            You can also reset the message and wait for another message to occur::

                with self.getLoggerStream('synapse.foo.bar', 'big badda boom happened') as stream:
                    # Do something that triggers a log message
                    doSomthing()
                    stream.wait(timeout=10)
                    stream.setMesg('yo dawg')  # This will now wait for the 'yo dawg' string to be written.
                    stream.wait(timeout=10)

                stream.seek(0)
                mesgs = stream.read()
                # Do something with messages

        Notes:
            This **only** captures logs for the current process.

        Yields:
            StreamEvent: A StreamEvent object
        '''
        stream = StreamEvent()
        stream.setMesg(mesg)
        handler = logging.StreamHandler(stream)
        slogger = logging.getLogger(logname)
        slogger.addHandler(handler)
        try:
            yield stream
        except:  # pragma: no cover
            raise
        finally:
            slogger.removeHandler(handler)

    @contextlib.contextmanager
    def setTstEnvars(self, **props):
        '''
        Set Environmental variables for the purposes of running a specific test.

        Args:
            **props: A kwarg list of envars to set. The values set are run
            through str() to ensure we're setting strings.

        Examples:
            Run a test while a envar is set::

                with self.setEnvars(magic='haha') as nop:
                    ret = dostuff()
                    self.true(ret)

        Notes:
            This helper explicitly sets and unsets values in os.environ, as
            os.putenv does not automatically updates the os.environ object.

        Yields:
            None. This context manager yields None. Upon exiting, envars are
            either removed from os.environ or reset to their previous values.
        '''
        old_data = {}
        pop_data = set()
        for key, valu in props.items():
            v = str(valu)
            oldv = os.environ.get(key, None)
            if oldv:
                if oldv == v:
                    continue
                else:
                    old_data[key] = oldv
                    os.environ[key] = v
            else:
                pop_data.add(key)
                os.environ[key] = v

        # This context manager is a nop
        try:
            yield None
        except:  # pragma: no cover
            raise
        # Clean up any new envars we set and any old envars we need to reset.
        finally:
            for key in pop_data:
                del os.environ[key]
            for key, valu in old_data.items():
                os.environ[key] = valu

    @contextlib.contextmanager
    def redirectStdin(self, new_stdin):
        '''
        Temporary replace stdin.

        Args:
            new_stdin(file-like object):  file-like object.

        Examples:
            inp = io.StringIO('stdin stuff\nanother line\n')
            with self.redirectStdin(inp):
                main()

            Here's a way to use this for code that's expecting the stdin buffer to have bytes.
            inp = Mock()
            inp.buffer = io.BytesIO(b'input data')
            with self.redirectStdin(inp):
                main()

        Returns:
            None
        '''
        old_stdin = sys.stdin
        sys.stdin = new_stdin
        yield
        sys.stdin = old_stdin

    def genraises(self, exc, gfunc, *args, **kwargs):
        '''
        Helper to validate that a generator function will throw an exception.

        Args:
            exc: Exception class to catch
            gfunc: Generator function to call.
            *args: Args passed to the generator function.
            **kwargs: Kwargs passed to the generator function.

        Notes:
            Wrap a generator function in a list() call and execute that in a
            bound local using ``self.raises(exc, boundlocal)``. The ``list()``
            will consume the generator until complete or an exception occurs.
        '''
        def testfunc():
            return list(gfunc(*args, **kwargs))

        self.raises(exc, testfunc)

    def eq(self, x, y, msg=None):
        '''
        Assert X is equal to Y
        '''
        if type(x) == list:
            x = tuple(x)

        if type(y) == list:
            y = tuple(y)

        self.assertEqual(x, y, msg=msg)

    def eqish(self, x, y, places=6, msg=None):
        '''
        Assert X is equal to Y within places decimal places
        '''
        self.assertAlmostEqual(x, y, places, msg=msg)

    def ne(self, x, y):
        '''
        Assert X is not equal to Y
        '''
        self.assertNotEqual(x, y)

    def true(self, x, msg=None):
        '''
        Assert X is True
        '''
        self.assertTrue(x, msg=msg)

    def false(self, x, msg=None):
        '''
        Assert X is False
        '''
        self.assertFalse(x, msg=msg)

    def nn(self, x, msg=None):
        '''
        Assert X is not None
        '''
        self.assertIsNotNone(x, msg=msg)

    def none(self, x, msg=None):
        '''
        Assert X is None
        '''
        self.assertIsNone(x, msg=msg)

    def noprop(self, info, prop):
        '''
        Assert a property is not present in a dictionary.
        '''
        valu = info.get(prop, s_common.novalu)
        self.eq(valu, s_common.novalu)

    def raises(self, *args, **kwargs):
        '''
        Assert a function raises an exception.
        '''
        return self.assertRaises(*args, **kwargs)

    async def asyncraises(self, exc, coro):
        with self.assertRaises(exc):
            await coro

    def sorteq(self, x, y, msg=None):
        '''
        Assert two sorted sequences are the same.
        '''
        return self.eq(sorted(x), sorted(y), msg=msg)

    def isinstance(self, obj, cls, msg=None):
        '''
        Assert a object is the instance of a given class or tuple of classes.
        '''
        self.assertIsInstance(obj, cls, msg=msg)

    def isin(self, member, container, msg=None):
        '''
        Assert a member is inside of a container.
        '''
        self.assertIn(member, container, msg=msg)

    def notin(self, member, container, msg=None):
        '''
        Assert a member is not inside of a container.
        '''
        self.assertNotIn(member, container, msg=msg)

    def gt(self, x, y, msg=None):
        '''
        Assert that X is greater than Y
        '''
        self.assertGreater(x, y, msg=msg)

    def ge(self, x, y, msg=None):
        '''
        Assert that X is greater than or equal to Y
        '''
        self.assertGreaterEqual(x, y, msg=msg)

    def lt(self, x, y, msg=None):
        '''
        Assert that X is less than Y
        '''
        self.assertLess(x, y, msg=msg)

    def le(self, x, y, msg=None):
        '''
        Assert that X is less than or equal to Y
        '''
        self.assertLessEqual(x, y, msg=msg)

    def len(self, x, obj, msg=None):
        '''
        Assert that the length of an object is equal to X
        '''
        gtyps = (s_telepath.Genr,
                 types.GeneratorType,
                 )
        if isinstance(obj, gtyps):
            obj = list(obj)

        self.eq(x, len(obj), msg=msg)

    def istufo(self, obj):
        '''
        Check to see if an object is a tufo.

        Args:
            obj (object): Object being inspected. This is validated to be a
            tuple of length two, contiaing a str or None as the first value,
            and a dict as the second value.

        Notes:
            This does not make any assumptions about the contents of the dictionary.

        Returns:
            None
        '''
        self.isinstance(obj, tuple)
        self.len(2, obj)
        self.isinstance(obj[0], (type(None), str))
        self.isinstance(obj[1], dict)

    @contextlib.contextmanager
    def getTestConfDir(self, name, boot=None, conf=None):
        with self.getTestDir() as dirn:
            cdir = os.path.join(dirn, name)
            s_common.makedirs(cdir)
            if boot:
                s_common.yamlsave(boot, cdir, 'boot.yaml')
            if conf:
                s_common.yamlsave(conf, cdir, 'cell.yaml')
            yield dirn

    def getTestCell(self, dirn, name, boot=None, conf=None):
        '''
        Get an instance of a Cell with specific boot and configuration data.

        Args:
            dirn (str): The directory the celldir is made in.
            name (str): The name of the cell to make. This must be a
            registered cell name in ``s_cells.ctors.``
            boot (dict): Optional boot data. This is saved to ``boot.yaml``
            for the cell to load.
            conf (dict): Optional configuration data. This is saved to
            ``cell.yaml`` for the Cell to load.

        Examples:

            Get a test Cortex cell:

                conf = {'key': 'value'}
                boot = {'cell:name': 'TestCell'}
                cell = getTestCell(someDirectory, 'cortex', conf, boot)

        Returns:
            s_cell.Cell: A Cell instance.
        '''
        cdir = os.path.join(dirn, name)
        s_common.makedirs(cdir)
        if boot:
            s_common.yamlsave(boot, cdir, 'boot.yaml')
        if conf:
            s_common.yamlsave(conf, cdir, 'cell.yaml')
        if name == 'cortex' and self.alt_write_layer:
            ldir = s_common.gendir(cdir, 'layers')
            layerdir = pathlib.Path(ldir, '000-default')
            os.symlink(self.alt_write_layer, layerdir)
        return s_cells.init(name, cdir)

    def getIngestDef(self, guid, seen):
        gestdef = {
            'comment': 'ingest_test',
            'source': guid,
            'seen': '20180102',
            'forms': {
                'teststr': [
                    '1234',
                    'duck',
                    'knight',
                ],
                'testint': [
                    '1234'
                ],
                'pivcomp': [
                    ('hehe', 'haha')
                ]
            },
            'tags': {
                'test.foo': (None, None),
                'test.baz': ('2014', '2015'),
                'test.woah': (seen - 1, seen + 1),
            },
            'nodes': [
                [
                    ['teststr',
                     'ohmy'
                    ],
                    {
                        'props': {
                            'bar': ('testint', 137),
                            'tick': '2001',
                        },
                        'tags': {
                            'beep.beep': (None, None),
                            'beep.boop': (10, 20),
                        }
                    }
                ],
                [
                    [
                        'testint',
                        '8675309'
                    ],
                    {
                        'tags': {
                            'beep.morp': (None, None)
                        }
                    }
                ]
            ],
            'edges': [
                [
                    [
                        'teststr',
                        '1234'
                    ],
                    'refs',
                    [
                        [
                            'testint',
                            1234
                        ]
                    ]
                ]
            ],
            'time:edges': [
                [
                    [
                        'teststr',
                        '1234'
                    ],
                    'wentto',
                    [
                        [
                            [
                            'testint',
                            8675309

                            ],
                            '20170102'
                        ]
                    ]
                ]
            ]
        }
        return gestdef

    def addCreatorDeleterRoles(self, core):
        '''
        Add two roles to a Cortex, the `creator` and `deleter` roles.
        Creator allows for node:add, prop:set and tag:add actions.
        Deleter allows for node:del, prop:del and tag:del actions.

        Args:
            core: Auth enabled cortex.
        '''
        core.addAuthRole('creator')
        core.addAuthRule('creator', (True, ('node:add',)))
        core.addAuthRule('creator', (True, ('prop:set',)))
        core.addAuthRule('creator', (True, ('tag:add',)))

        core.addAuthRole('deleter')
        core.addAuthRule('deleter', (True, ('node:del',)))
        core.addAuthRule('deleter', (True, ('prop:del',)))
        core.addAuthRule('deleter', (True, ('tag:del',)))

    @contextlib.contextmanager
    def getTestDmonCortexAxon(self, rootperms=True):
        '''
        Get a test Daemon with a Cortex and a Axon with a single BlobStor
        enabled. The Cortex is an auth enabled cortex with the root username
        and password as "root:root".

        This environment can be used to run tests which require having both
        an Cortex and a Axon readily available.

        Valid connection URLs for the Axon and Cortex are set in the local
        scope as "axonurl" and "coreurl" respectively.

        Args:
            perms (bool): If true, grant the root user * permissions on the Cortex.

        Returns:
            s_daemon.Daemon: A configured Daemon.
        '''
        with self.getTestDmon('axoncortexdmon') as dmon:

            # Construct URLS for later use
            blobstorurl = f'tcp://{dmon.addr[0]}:{dmon.addr[1]}/blobstor00'
            axonurl = f'tcp://{dmon.addr[0]}:{dmon.addr[1]}/axon00'
            coreurl = f'tcp://root:root@{dmon.addr[0]}:{dmon.addr[1]}/core'

            # register the blob with the Axon.
            with dmon._getTestProxy('axon00') as axon:
                axon.addBlobStor(blobstorurl)

            # Add our helper URLs to scope so others don't
            # have to construct them.
            s_scope.set('axonurl', axonurl)
            s_scope.set('coreurl', coreurl)
            s_scope.set('blobstorurl', blobstorurl)

            # grant the root user permissions
            if rootperms:
                with dmon._getTestProxy('core', user='root', passwd='root') as core:
                    self.addCreatorDeleterRoles(core)
                    core.addUserRole('root', 'creator')
                    core.addUserRole('root', 'deleter')

            yield dmon

class SyncToAsyncCMgr():
    ''' Wraps a regular context manager in an async one '''
    def __init__(self, func, *args, **kwargs):
        def run_and_enter():
            obj = func(*args, **kwargs)
            rv = obj.__enter__()
            return obj, rv

        self.coro = s_glob.plex.executor(run_and_enter)
        self.obj = None

    async def __aenter__(self):
        self.obj, rv = await self.coro
        return rv

    async def __aexit__(self, *args):
        return await s_glob.plex.executor(self.obj.__exit__, *args)
