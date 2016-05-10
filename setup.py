#!/usr/bin/env python3

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

from setuptools import setup

setup(
    name="userspacefs",
    description="User-space file systems",
    url='https://github.com/rianhunter/userspacefs',
    license="GPL3",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    packages=["userspacefs"],
    install_requires=[
        "fusepy>=2,<3",
    ],
)
