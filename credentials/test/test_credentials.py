import sys
sys.path.append("../")

try:
    from credentials import credentials
    prefix = "credentials.credentials."
except:
    import credentials
    prefix = "credentials."

import unittest
from unittest.mock import patch, call, create_autospec, Mock

@patch(prefix + 'bossutils.vault.Vault', autospec = True)
@patch(prefix + 'os', autospec = True)
@patch(prefix + 'socket', autospec = True)
@patch(prefix + 'signal', autospec = True)
class TestCredentialsService(unittest.TestCase):
    def test_instantiation_setup(self, mSig, mSock, mOs, mVault):
        iVault = mVault.return_value
        iVault.config = {"system":{"type": "test"}}

        mSig.SIGALRM = 1
        credentials.EXIT_SIGNAL = 2

        # make the environment dirty
        mOs.path.dirname.return_value = None
        mOs.path.isdir.return_value = False
        mOs.path.exists.return_value = True


        creds = credentials.Credentials()


        # Verify results
        assert(creds.aws_path == "aws/creds/test")

        expected = [call(1, creds.sig_handler), call(2, creds.sig_handler)]
        assert(mSig.signal.call_args_list == expected)

        expected = [call(None, 0o755), call(credentials.SOCKET_NAME, 0o777)]
        assert(mOs.chmod.call_args_list == expected)

        sock = mSock.socket.return_value
        sock.bind.assert_called_once_with(credentials.SOCKET_NAME)
        sock.listen.assert_called_once_with(credentials.SOCKET_BACKLOG)

    @patch(prefix + 'open', create = True)
    def test_daemonize(self, mOpen, mSig, mSock, mOs, mVault):
        iVault = mVault.return_value
        iVault.config = {"system":{"type": "test"}}

        mOs.getpid.return_value = 0
        mOs.fork.return_value = 0 # the child process always gets pid 0

        creds = credentials.Credentials()
        creds.daemonize()

        # check that daemonize double forks
        assert(mOs.fork.call_count == 2)

        # check that the pid is written to disk
        expected = [call(credentials.PID_FILE, "w+"),
                    call().__enter__(), # enter the context manager
                    call().__enter__().write("0\n"), # the actual write
                    call().__exit__(None, None, None)]
        assert(mOpen.mock_calls == expected)

    @patch(prefix + 'bossutils.utils.set_excepthook', autospec = True)
    def test_main_loop_request_and_exit(self, mHook, mSig, mSock, mOs, mVault):
        iVault = mVault.return_value
        iVault.config = {"system":{"type": "test"}}

        conn = Mock()
        mSock.socket.return_value.accept.side_effect = [(conn, None), InterruptedError()]

        mOs.path.exists.return_value = False

        creds = credentials.Credentials()
        # Stub out internal methods so only run is tested
        creds.daemonize = create_autospec(creds.daemonize)
        creds.renew_or_request = create_autospec(creds.renew_or_request)
        creds.creds = {"data":{}, "lease_id":True}

        creds.run()

        creds.daemonize.assert_called_once_with()
        mHook.assert_called_once_with()
        creds.renew_or_request.assert_called_once_with()

        conn.sendall.assert_called_once_with(b'{}')
        conn.close.assert_called_once_with()

        sock = mSock.socket.return_value
        assert(sock.accept.call_count == 2)
        assert(sock.close.call_count == 1)
        assert(creds.sock == None)

        iVault.revoke_secret.assert_called_once_with(True)
        assert(creds.creds == None)

        expected = [call(credentials.SOCKET_NAME), call(credentials.PID_FILE)]
        assert(mOs.remove.call_args_list == expected)

    def test_renew_credentials(self, mSig, mSock, mOs, mVault):
        iVault = mVault.return_value
        iVault.config = {"system":{"type": "test"}}

        creds = credentials.Credentials()
        creds.creds = {"lease_id":True, "lease_duration": 0}

        creds.renew_or_request()

        iVault.renew_secret.assert_called_once_with(True)

    @patch(prefix + 'time.sleep', autospec = True)
    def test_request_credentials(self, mSleep, mSig, mSock, mOs, mVault):
        iVault = mVault.return_value
        iVault.config = {"system":{"type": "test"}}

        creds = credentials.Credentials()
        creds.creds = {"lease_id":False, "lease_duration": 0}

        creds.renew_or_request()

        iVault.read_dict.assert_called_once_with(creds.aws_path, raw=True)
        mSleep.assert_called_once_with(15)

    def testset_renew_alarm(self, mSig, mSock, mOs, mVault):
        iVault = mVault.return_value
        iVault.config = {"system":{"type": "test"}}

        creds = credentials.Credentials()
        creds.creds = {"lease_id":True, "lease_duration": 100}

        creds.renew_or_request()

        mSig.alarm.assert_called_once_with(85)

    def test_sig_handler_alarm(self, mSig, mSock, mOs, mVault):
        iVault = mVault.return_value
        iVault.config = {"system":{"type": "test"}}

        mSig.SIGALRM = 1

        creds = credentials.Credentials()
        creds.renew_or_request = create_autospec(creds.renew_or_request)

        creds.sig_handler(1, None)

        creds.renew_or_request.assert_called_once_with()

    def test_sig_handler_exit_sig(self, mSig, mSock, mOs, mVault):
        iVault = mVault.return_value
        iVault.config = {"system":{"type": "test"}}

        credentials.EXIT_SIGNAL = 1

        creds = credentials.Credentials()

        with self.assertRaises(InterruptedError):
            creds.sig_handler(1, None)
