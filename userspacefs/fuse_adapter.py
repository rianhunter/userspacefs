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

import contextlib
import datetime
import errno
import itertools
import logging
import os
import random
import threading
import stat
import sys

from fusepyng import FUSE, LoggingMixIn

from userspacefs.util_dumpster import utctimestamp

log = logging.getLogger(__name__)

class AttrCaller(object):
    def __call__(self, op, *args):
        return getattr(self, op)(*args)

def check_mode(mode):
    if ((stat.S_IFCHR | stat.S_IFBLK | stat.S_IFIFO | stat.S_IFLNK | stat.S_IFSOCK) & mode) == (stat.S_IFCHR | stat.S_IFBLK | stat.S_IFIFO | stat.S_IFLNK | stat.S_IFSOCK):
        raise OSError(errno.EPERM, os.strerror(errno.EPERM))

# Can't derive from fuse.Operations because Finder will
# fail to copy if getxattr() returns ENOTSUP, better to
# not implement it at all
class FUSEAdapter(LoggingMixIn, AttrCaller):
    flag_nopath = 1

    def __init__(self, create_fs, on_init=None):
        self._create_fs = create_fs
        self._fh_to_file = {}
        self._lock = threading.Lock()
        self._on_init = on_init
        self._fs = None

    def _save_file(self, f):
        with self._lock:
            while True:
                r = random.randint(0, 2 ** 32 - 1)
                if r not in self._fh_to_file:
                    break
            self._fh_to_file[r] = f
            return r

    def _delete_file(self, fh):
        with self._lock:
            return self._fh_to_file.pop(fh)

    def _conv_path(self, path):
        toret = self._fs.create_path()
        if path == '/':
            return toret
        return toret.joinpath(*path[1:].split('/'))

    def _fs_stat_to_fuse_attrs(self, st):
        toret = {}

        toret['st_birthtime'] = utctimestamp(getattr(st, "birthtime", datetime.datetime.utcfromtimestamp(0)))
        toret['st_mtime'] = utctimestamp(getattr(st, "mtime", datetime.datetime.utcfromtimestamp(toret['st_birthtime'])))
        toret['st_ctime'] = utctimestamp(getattr(st, "ctime", datetime.datetime.utcfromtimestamp(toret['st_mtime'])))
        toret['st_atime'] = utctimestamp(getattr(st, "atime", datetime.datetime.utcfromtimestamp(toret['st_ctime'])))

        toret['st_size'] = st.size

        toret['st_mode'] = ((stat.S_IFDIR | 0o777)
                            if st.type == 'directory' else
                            (stat.S_IFREG | 0o777))

        # NB: st_nlink on directories is really inconsistent across filesystems
        #     and OSes. it arguably doesn't matter at all but we set it to
        #     non-zero just in case
        toret['st_nlink'] = 1
        toret['st_uid'] = os.getuid()
        toret['st_gid'] = os.getgid()

        return toret

    def init(self, _):
        if self._on_init is not None:
            self._on_init()
        self._fs = self._create_fs()

    def destroy(self, _):
        if self._fs is not None:
            self._fs.close()
            self._fs = None

    def getattr(self, path, fh=None):
        if fh is not None:
            fh = fh.fh

        if fh is not None:
            if fh not in self._fh_to_file:
                raise Exception("Fuse passed us invalid file handle!: %r" % (fh,))
            st = self._fs.fstat(self._fh_to_file[fh])
        else:
            st = self._fs.stat(self._conv_path(path))
        return self._fs_stat_to_fuse_attrs(st)

    def mknod(self, path, mode, dev):
        # Not all fuse implementations call create()
        check_mode(mode)
        self._fs.open(self._conv_path(path),
                      os.O_WRONLY | os.O_CREAT).close()

    def create(self, path, mode, fi):
        check_mode(mode)
        fi.fh = self._save_file(self._fs.open(self._conv_path(path), fi.flags))
        return 0

    def open(self, path, fi):
        fi.fh = self._save_file(self._fs.open(self._conv_path(path), fi.flags))
        return 0

    def read(self, path, size, offset, fh):
        fh = fh.fh
        f = self._fh_to_file[fh]
        return self._fs.pread(f, size, offset)

    def write(self, path, data, offset, fh):
        fh = fh.fh
        f = self._fh_to_file[fh]
        return self._fs.pwrite(f, data, offset)

    def truncate(self, path, length, fh=None):
        if fh is not None:
            fh = fh.fh

        if fh is None:
            # TODO: add truncate() call to FS interface
            with contextlib.closing(self._fs.open(self._conv_path(path), os.O_WRONLY)) as f:
                self._fs.ftruncate(f, length)
        else:
            f = self._fh_to_file[fh]
            self._fs.ftruncate(f, length)

    def fsync(self, path, datasync, fh):
        fh = fh.fh
        self._fs.fsync(self._fh_to_file[fh])
        return 0

    def release(self, path, fh):
        fh = fh.fh
        self._delete_file(fh).close()
        return 0

    def opendir(self, path):
        return self._save_file(self._fs.open_directory(self._conv_path(path)))

    def readdir(self, path, fh):
        # TODO: pyfuse doesn't expose a better interface for large directories
        f = self._fh_to_file[fh]
        return list(itertools.chain(['.', '..'], map(lambda x: (x.name, self._fs_stat_to_fuse_attrs(x), 0), f)))

    def releasedir(self, path, fh):
        self._delete_file(fh).close()

    def unlink(self, path):
        self._fs.unlink(self._conv_path(path))

    def mkdir(self, path, mode):
        self._fs.mkdir(self._conv_path(path))

    def rmdir(self, path):
        self._fs.rmdir(self._conv_path(path))

    def rename(self, oldpath, newpath):
        oldpath_ = self._conv_path(oldpath)
        newpath_ = self._conv_path(newpath)
        while True:
            try:
                self._fs.rename_noreplace(oldpath_, newpath_)
            except FileExistsError:
                try:
                    self._fs.unlink(newpath_)
                except FileNotFoundError:
                    pass
            else:
                break

    def chmod(self, path, mode):
        return 0

    def utimens(self, path, times=None):
        try:
            fn = self._fs.x_f_set_file_times
        except AttributeError:
            # NB: x_f_set_file_times is an optional call
            return 0

        with contextlib.closing(self._fs.open(self._conv_path(path), os.O_RDONLY)) as f:
            fn(f,
               None,
               None if times is None else datetime.datetime.utcfromtimestamp(times[0]),
               None if times is None else datetime.datetime.utcfromtimestamp(times[1]),
               None)
        return 0

    def statfs(self, _):
        vfs = self._fs.statvfs()
        toret = {}
        for n in ['f_bavail', 'f_blocks', 'f_frsize']:
            toret[n] = getattr(vfs, n)

        toret['f_bfree'] = toret['f_bavail']
        toret['f_bsize'] = toret['f_frsize']

        return toret

def run_fuse_mount(create_fs, mount_point, foreground=False, display_name=None, fsname=None, on_init=None, **kw):
    if sys.platform == 'darwin':
        kw['volname'] = display_name
    FUSE(FUSEAdapter(create_fs, on_init=on_init),
         mount_point, foreground=foreground, hard_remove=True,
         default_permissions=True, fsname=fsname,
         raw_fi=True,
         **kw)
