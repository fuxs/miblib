# -*- coding: utf-8 -*-
# Copyright 2024 Michael Bungenstock
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from setuptools import setup, find_packages

VERSION = "0.0.1"
DESCRIPTION = "miblib, Michael's library"
LONG_DESCRIPTION = "miblib is a collection of usefull Python functions"

dependencies = [
    "google-cloud-bigquery >=  3.25.0",
    "google-cloud-bigquery-storage >= 2.26.0",
]
url = "https://github.com/fuxs/miblib"

setup(
    name="miblib",
    version=VERSION,
    description=DESCRIPTION,
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Michael Bungenstock",
    author_email="sommerlich.ausrichtet.0l@icloud.com",
    packages=find_packages(),
    install_requires=dependencies,
    url=url,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    keywords=["python"],
    platforms="Posix; MacOS X; Windows",
    python_requires=">=3.7",
)
