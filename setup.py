#!/usr/bin/env python3
#
# Copyright 2021 RelationalAI, Inc.
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
# limitations under the License

from setuptools import setup

import railib

setup(
    author="RelationalAI, Inc.",
    author_email="support@relational.ai",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="The RelationalAI Software Development Kit for Python",
    install_requires=["ed25519==1.5"],
    license="http://www.apache.org/licenses/LICENSE-2.0",
    long_description="Enables access to the RelationalAI REST APIs from Python",
    long_description_content_type="text/markdown",
    name="rai-sdk",
    packages=["railib"],
    url="http://github.com/RelationalAI/rai-sdk-python",
    version=railib.__version__)
