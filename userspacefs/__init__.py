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

import errno
import functools
import importlib
import logging
import os
import pathlib
import queue
import random
import re
import signal
import socket
import subprocess
import sys
import threading

try:
    from userspacefs.fuse_adapter import run_fuse_mount
except EnvironmentError:
    run_fuse_mount = None

from userspacefs.macos_path_conversion import FileSystem as MacOSPathConversionFileSystem
from userspacefs.smbserver import SMBServer

log = logging.getLogger(__name__)

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

def get_func(fully_qualified_fn_name):
    (create_fs_module, fn_name) = fully_qualified_fn_name.rsplit('.', 1)
    return getattr(importlib.import_module(create_fs_module), fn_name)

def create_create_fs(create_fs_params):
    (create_fs_module, fs_args) = create_fs_params

    create_fs_ = get_func(create_fs_module)

    create_fs = functools.partial(create_fs_, fs_args)

    if sys.platform == "darwin":
        orig_create_fs = create_fs
        def create_fs_():
            return MacOSPathConversionFileSystem(orig_create_fs())

        create_fs = create_fs_

    return create_fs

def run_smb_server(create_fs_params, mount_signal,
                   display_name=None,
                   smb_no_mount=False,
                   smb_listen_address=None,
                   mount_point=None,
                   foreground=False):
    if create_fs_params is None:
        raise Exception("need create_fs_module value in environment")

    if display_name is None:
        raise Exception("need display name argument!")

    create_fs = create_create_fs(create_fs_params)

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

    server = None

    mm_q = queue.Queue()
    def check_mount():
        is_mounted = False

        if not smb_no_mount:
            ret = subprocess.call(["mount", "-t", "smbfs",
                                   "cifs://guest:@127.0.0.1:%d/%s" %
                                   (port, display_name),
                                   mount_point])
            if ret:
                log.debug("Mount failed, killing server")
                server.close()
                return

            is_mounted = True

        # give mount signal
        mount_signal((host, port))

        while True:
            try:
                r = mm_q.get(timeout=(None
                                      if not is_mounted else
                                      1 if foreground else 30))
            except queue.Empty:
                pass
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

        threading.Thread(target=check_mount, daemon=True).start()

        server.run()
    finally:
        fs.close()

def run_mount(create_fs_params, mount_point,
              foreground=False,
              display_name=None,
              fuse_options=None,
              smb_no_mount=False,
              smb_listen_address=None,
              smb_only=False,
              mount_signal=None):
    if not smb_only and run_fuse_mount is not None:
        log.debug("Attempting fuse mount")
        try:
            if fuse_options is None:
                fuse_options = {}

            # foreground=True here is not a error, run_mount() always runs in
            # foreground, the kwarg is for run_smb_server()
            run_fuse_mount(create_create_fs(create_fs_params), mount_point, foreground=True,
                           display_name=display_name, fsname=display_name, on_init=mount_signal,
                           **fuse_options)
            return 0
        except RuntimeError as e:
            # Fuse is broken, fall back to SMB
            pass

    run_smb_server(create_fs_params, mount_signal=mount_signal,
                   smb_no_mount=smb_no_mount, smb_listen_address=smb_listen_address,
                   mount_point=mount_point, display_name=display_name, foreground=foreground)

    return 0

def main_(argv=None):
    if argv is None:
        argv = sys.argv

    # to avoid accidental SIGPIPE
    # redirect stdout to devnull
    # we have to do it this way because pass_fds is not
    # supported on windows, but dup() is
    new_stdout = os.fdopen(os.dup(1), "w")
    os.close(1)
    fd = os.open(os.devnull, os.O_WRONLY)
    if fd != 1:
        os.dup2(fd, 1)
        os.close(fd)

    create_fs_module = None
    fs_args = {}
    smb_no_mount = False
    smb_listen_address = None
    mount_point = None
    display_name = None
    on_new_process = None
    proc_args = {}
    smb_only = False
    fuse_options = {}

    # get fs_args from env
    for (key, value) in os.environ.items():
        if  key.startswith("__userspacefs_fs_arg_"):
            fs_args[key[len("__userspacefs_fs_arg_"):]] = value
        elif key.startswith("__userspacefs_proc_arg_"):
            proc_args[key[len("__userspacefs_proc_arg_"):]] = value
        elif key == "__userspacefs_onp_module":
            on_new_process = value
        elif key == "__userspacefs_create_fs_module":
            create_fs_module = value
        elif key == "__userspacefs_smb_no_mount":
            smb_no_mount = True
        elif key == "__userspacefs_smb_listen_address":
            smb_listen_address = value.split(":", 1)
            if len(smb_listen_address) == 1:
                smb_listen_address = list(smb_listen_address) + [None]
            else:
                smb_listen_address = (smb_listen_address[0], int(smb_listen_address[1]))
        elif key == "__userspacefs_mount_point":
            mount_point = value
        elif key == "__userspacefs_display_name":
            display_name = value
        elif key == "__userspacefs_smb_only":
            smb_only = True
        elif key.startswith("__userspacefs_fuse_opt_"):
            fuse_options[key[len("__userspacefs_fuse_opt_"):]] = value

    if on_new_process is not None:
        get_func(on_new_process)(proc_args)

    create_fs_params = (create_fs_module, fs_args)

    def mount_signal(hostport=None):
        if hostport is not None:
            (host, port) = hostport
            print("mounted %s %d" % (host, port), file=new_stdout, flush=True)
        else:
            print("mounted", file=new_stdout, flush=True)
        # new_stdout will not be used anymore
        new_stdout.close()

    run_mount(create_fs_params, mount_point,
              foreground=False,
              mount_signal=mount_signal,
              display_name=display_name,
              smb_no_mount=smb_no_mount,
              smb_listen_address=smb_listen_address,
              smb_only=smb_only,
              fuse_options=fuse_options)

    return 0

def main(argv=None):
    try:
        return main_(argv=argv)
    except Exception:
        logging.exception("unexpected exception")
        return -1

def mount_and_run_fs(display_name, create_fs_params, mount_point,
                     on_new_process=None,
                     foreground=False,
                     smb_only=False,
                     smb_no_mount=False,
                     smb_listen_address=None,
                     fuse_options=None):
    assert smb_no_mount or mount_point is not None

    if not smb_no_mount:
        mount_point = os.path.abspath(mount_point)

    # smb_no_mount implies smb
    if smb_no_mount:
        smb_only = True

    can_mount_smb_automatically = sys.platform == "darwin"
    if (smb_only or run_fuse_mount is None) and not smb_no_mount and not can_mount_smb_automatically:
        raise MountError("Unable to mount file system")

    def no_auto_mount_message(hostport=None):
        if smb_no_mount:
            assert hostport is not None
            (host, port) = hostport
            print("You can access the SMB server at cifs://guest:@%s:%d/%s" %
                  (host,
                   port,
                   display_name))

    if not foreground:
        if sys.executable is None:
            raise Exception("need a path to the executable!")

        for (key, value) in create_fs_params[1].items():
            os.environ['__userspacefs_fs_arg_' + key] = value

        os.environ['__userspacefs_create_fs_module'] = create_fs_params[0]

        if on_new_process is not None:
            for (key, value) in on_new_process[1].items():
                os.environ['__userspacefs_proc_arg_' + key] = value

            os.environ['__userspacefs_onp_module'] = on_new_process[0]
        if smb_no_mount:
            os.environ['__userspacefs_smb_no_mount'] = '1'
        if smb_listen_address is not None:
            ser = smb_listen_address[0]
            if smb_listen_address[1] is not None:
                ser += ":%d" % (smb_listen_address[1],)
            os.environ['__userspacefs_smb_listen_address'] = ser
        if mount_point is not None:
            os.environ["__userspacefs_mount_point"] = mount_point
        if display_name is not None:
            os.environ["__userspacefs_display_name"] = display_name
        if fuse_options is not None:
            for (key, value) in fuse_options.items():
                os.environ['__userspacefs_fuse_opt_' + key] = value
        if smb_only:
            os.environ['__userspacefs_smb_only'] = '1'

        proc = subprocess.Popen([sys.executable, __file__],
                                text=1,
                                stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                                start_new_session=True,
                                cwd=pathlib.Path.cwd().root)

        # wait for proc to get mounted\n (signaled by writing mounted stdout)
        # or for proc to die
        ret = 1
        while True:
            buf = proc.stdout.readline()
            if not buf:
                ret = proc.poll()
                break

            mo = re.search(r"^mounted(\s+(\d+\.\d+\.\d+\.\d+)\s+(\d+))?\s*$", buf)
            if mo is not None:
                if mo[1] is not None:
                    hostport = (mo[2], int(mo[3]))
                else:
                    hostport = None
                no_auto_mount_message(hostport)
                ret = 0
                break

        return ret

    run_mount(create_fs_params, mount_point,
              foreground=True,
              mount_signal=no_auto_mount_message,
              display_name=display_name,
              fuse_options=fuse_options,
              smb_no_mount=smb_no_mount,
              smb_listen_address=smb_listen_address,
              smb_only=smb_only)

    return 0

def simple_main(mount_point, display_name, create_fs_params,
                on_new_process=None,
                foreground=False,
                smb_only=False,
                smb_no_mount=False,
                smb_listen_address=None,
                fuse_options=None):
    try:
        return mount_and_run_fs(display_name, create_fs_params,
                                mount_point,
                                on_new_process=on_new_process,
                                foreground=foreground,
                                smb_only=smb_only,
                                smb_no_mount=smb_no_mount,
                                smb_listen_address=smb_listen_address,
                                fuse_options=fuse_options)
    except MountError as e:
        print(e)
        return -1

if __name__ == "__main__":
    sys.exit(main(sys.argv))
