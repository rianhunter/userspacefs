#!/usr/bin/env python3

# This file is part of userspacefs.

# userspacefs is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# userspacefs is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with userspacefs.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import errno
import logging
import os
import queue
import random
import signal
import socket
import subprocess
import sys
import syslog
import threading

try:
    from userspacefs.fuse_adapter import run_fuse_mount
except EnvironmentError:
    run_fuse_mount = None

from userspacefs.macos_path_conversion import FileSystem as MacOSPathConversionFileSystem
from userspacefs.smbserver import SMBServer

log = logging.getLogger(__name__)

def daemonize():
    res = os.fork()
    if res:
        return res

    os.setsid()

    os.chdir("/")

    nullfd = os.open("/dev/null", os.O_RDWR)
    try:
        os.dup2(nullfd, 0)
        os.dup2(nullfd, 1)
        os.dup2(nullfd, 2)
    finally:
        os.close(nullfd)

class SimpleSMBBackend(object):
    def __init__(self, path, fs):
        self._path = path
        self._fs = fs

    def tree_connect(self, server, path):
        if path.rsplit("\\", 1)[-1].upper() == self._path.rsplit("\\", 1)[-1].upper():
            return self._fs
        raise KeyError()

    def tree_disconnect(self, server, fs):
        pass

    def tree_disconnect_hard(self, server, fs):
        pass

class MountError(Exception): pass

def mount_and_run_fs(display_name, create_fs, mount_point,
                     foreground=False,
                     smb_only=False,
                     smb_no_mount=False,
                     smb_listen_address=None,
                     on_new_process=None,
                     fuse_options=None):
    assert smb_no_mount or mount_point is not None

    if not smb_no_mount:
        mount_point = os.path.abspath(mount_point)

    # smb_no_mount implies smb
    if smb_no_mount:
        smb_only = True

    if not smb_only and run_fuse_mount is not None:
        log.debug("Attempting fuse mount")
        try:
            if fuse_options is None:
                fuse_options = {}
            run_fuse_mount(create_fs, mount_point, foreground=foreground,
                           display_name=display_name, fsname=display_name,
                           on_init=None if foreground else on_new_process,
                           **fuse_options)
            return 0
        except RuntimeError as e:
            # Fuse is broken, fall back to SMB
            pass

    can_mount_smb_automatically = sys.platform == "darwin"
    if not smb_no_mount and not can_mount_smb_automatically:
        raise MountError("Unable to mount file system")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    if smb_listen_address is None:
        (host, port) = ("127.0.0.1", None)
    else:
        (host, port) = smb_listen_address

    if port is None:
        while True:
            port = random.randint(60000, 2 ** 16)
            try:
                sock.bind((host, port))
            except OSError as err:
                if err.errno != errno.EADDRINUSE: raise
            else:
                break
    else:
        for prop in ('SO_REUSEADDR', 'SO_REUSEPORT'):
            if hasattr(socket, prop):
                sock.setsockopt(socket.SOL_SOCKET, getattr(socket, prop), True)

        sock.bind((host, port))

    if sys.platform == "darwin":
        orig_create_fs = create_fs
        def create_fs():
            return MacOSPathConversionFileSystem(orig_create_fs())

    if smb_no_mount:
        print("You can access the SMB server at cifs://guest:@%s:%d/%s" %
              (host,
               port,
               display_name))

    (r, w) = os.pipe()

    def mount_notify(child_pid):
        # wait for server to startup before attempting mount
        os.read(r, 1)
        if not smb_no_mount:
            ret = subprocess.call(["mount", "-t", "smbfs",
                                   "cifs://guest:@127.0.0.1:%d/%s" %
                                   (port, display_name),
                                   mount_point])
            if ret:
                log.debug("Mount failed, Sending kill signal!")
                os.kill(child_pid, signal.SIGTERM)
            else:
                log.debug("Mount succeeded, Sending mounted signal!")
                os.kill(child_pid, signal.SIGUSR1)
        else:
            ret = 0
        return ret

    if not foreground:
        child_pid = daemonize()

        if child_pid:
            os.close(w)
            return mount_notify(child_pid)
        elif on_new_process is not None:
            on_new_process()

        os.close(r)
    else:
        threading.Thread(target=mount_notify, args=(os.getpid(),), daemon=True).start()

    server = None

    mm_q = queue.Queue()
    def check_mount():
        is_mounted = False
        while True:
            try:
                r = mm_q.get(timeout=(None
                                      if not is_mounted else
                                      1 if foreground else 30))
            except queue.Empty:
                pass
            else:
                if r:
                    log.debug("Setting is_mounted!")
                    is_mounted = True
                else:
                    log.debug("Got kill flag!")
                    break

            if is_mounted and not os.path.ismount(mount_point):
                log.debug("Drive has gone unmounted")
                is_mounted = False
                break

        if is_mounted:
            subprocess.call(["umount", "-f", mount_point])

        log.debug("CALLING SERVER CLOSE")
        server.close()

    def handle_mounted(self, *_):
        log.debug("Got mounted signal!")
        mm_q.put(True)

    def kill_signal(self, *_):
        log.debug("Got kill signal!")
        mm_q.put(False)

    fs = create_fs()
    try:
        server = SMBServer(SimpleSMBBackend("\\\\127.0.0.1\\%s" % (display_name,),
                                            fs),
                           sock=sock)

        # enable signals now that server is set
        signal.signal(signal.SIGTERM, kill_signal)
        signal.signal(signal.SIGINT, kill_signal)
        signal.signal(signal.SIGUSR1, handle_mounted)

        # give mount signal
        os.write(w, b'\0')

        threading.Thread(target=check_mount, daemon=True).start()

        server.run()
    finally:
        fs.close()

class RealSysLogHandler(logging.Handler):
    def __init__(self, *n, **kw):
        super().__init__()
        syslog.openlog(*n, **kw)

    def _map_priority(self, levelname):
        return {
            logging.DEBUG:    syslog.LOG_DEBUG,
            logging.INFO:     syslog.LOG_INFO,
            logging.ERROR:    syslog.LOG_ERR,
            logging.WARNING:  syslog.LOG_WARNING,
            logging.CRITICAL: syslog.LOG_CRIT,
            }[levelname]

    def emit(self, record):
        msg = self.format(record)
        priority = self._map_priority(record.levelno)
        syslog.syslog(priority, msg)

class FUSEOption(argparse.Action):
    def __init__(self, **kw):
        super(FUSEOption, self).__init__(**kw)

    def __call__(self, parser, ns, values, option_string):
        if ns.o is None:
            ns.o = {}
        for kv in values.split(","):
            ret = kv.split("=", 1)
            if len(ret) == 2:
                ns.o[ret[0]] = ret[1]
            else:
                ns.o[ret[0]] = True

def add_cli_arguments(parser):
    def ensure_listen_address(string):
        try:
            (host, port) = string.split(":", 1)
        except ValueError:
            try:
                port = int(string)
                if not (0 < port < 65536):
                    raise ValueError()
            except ValueError:
                host = string
                port = None
            else:
                host = ''
        else:
            if port:
                port = int(port)
                if not (0 < port < 65536):
                    raise argparse.ArgumentTypeError("%r is not a valid TCP port" % (port,))
            else:
                port = None

        if not host:
            host = "127.0.0.1"

        return (host, port)

    parser.add_argument("-f", "--foreground", action="store_true",
                        help="keep filesystem server in foreground")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="show log messages, use twice for maximum verbosity")
    parser.add_argument("-s", "--smb", action="store_true",
                        help="force mounting via SMB")
    parser.add_argument("-n", "--smb-no-mount", action="store_true",
                        help="export filesystem via SMB but don't mount it")
    parser.add_argument("-l", "--smb-listen-address", default="127.0.0.1",
                        type=ensure_listen_address,
                        help="address that SMB service should listen on, append colon to specify port")
    parser.add_argument("-o", metavar='opt,[opt...]', action=FUSEOption,
                        help="FUSE options, e.g. -o uid=1000,allow_other")

def simple_main(mount_point, display_name, create_fs, args=None, argv=None, on_new_process=None):
    if args is None:
        if argv is None:
            argv = sys.argv

        parser = argparse.ArgumentParser()
        add_cli_arguments(parser)
        args = parser.parse_args(argv[1:])

    if args.foreground:
        format_ = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
        logging_stream = logging.StreamHandler()
    else:
        format_ = '%(levelname)s:%(name)s:%(message)s'
        logging_stream = RealSysLogHandler(display_name, syslog.LOG_PID)

    level = [logging.WARNING, logging.INFO, logging.DEBUG][min(2, args.verbose)]
    logging.basicConfig(level=level, handlers=[logging_stream], format=format_)

    try:
        return mount_and_run_fs(display_name, create_fs,
                                mount_point,
                                foreground=args.foreground,
                                smb_only=args.smb,
                                smb_no_mount=args.smb_no_mount,
                                smb_listen_address=args.smb_listen_address,
                                on_new_process=on_new_process,
                                fuse_options=args.o)
    except MountError as e:
        print(e)
        return -1
