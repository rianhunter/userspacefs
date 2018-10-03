# This file is part of userspacefs.

# userspacefs is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# userspacefs is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with userspacefs.  If not, see <http://www.gnu.org/licenses/>.

# macOS SMB client converts illegal Windows characters like so:
#   0x01-0x1F       0xF001-0xF01F
#  "               0xF020
#  *               0xF021
#  /               0xF022
#  <               0xF023
#  >               0xF024
#  ?               0xF025
#  \               0xF026
#  |               0xF027

REPLACE_MAP = {
    0xf020: ord('"'),
    0xf021: ord('*'),
    0xf022: ord('/'),
    0xf023: ord('<'),
    0xf024: ord('>'),
    0xf025: ord('?'),
    0xf026: ord('\\'),
    0xf027: ord('|'),
}

for i in range(0x1, 0x20):
    REPLACE_MAP[0xF000 | i] = i

class FileSystem(object):
    def __init__(self, backing_fs):
        self._sub = backing_fs

    def _convert_path(self, path):
        return self._sub.create_path(*[p.translate(REPLACE_MAP) for p in path.parts[1:]])

    def open(self, path, *n, **kw):
        return self._sub.open(self._convert_path(path), *n, **kw)

    def open_directory(self, path, *n, **kw):
        return self._sub.open_directory(self._convert_path(path), *n, **kw)

    def stat(self, path):
        return self._sub.stat(self._convert_path(path))

    def unlink(self, path):
        return self._sub.unlink(self._convert_path(path))

    def mkdir(self, path):
        return self._sub.mkdir(self._convert_path(path))

    def rmdir(self, path):
        return self._sub.rmdir(self._convert_path(path))

    def rename_noreplace(self, old_path, new_path):
        return self._sub.rename_noreplace(self._convert_path(old_path),
                                          self._convert_path(new_path))

    def __getattr__(self, name):
        return getattr(self._sub, name)
