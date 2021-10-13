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
# limitations under the License.

from abc import ABC
import time


__all__ = [
    "Credentials",
    "AccessKeyCredentials",
    "AccessToken",
    "ClientCredentials"
]

DEFAULT_CLIENT_CREDENTIALS_URL = "https://login.relationalai.com/oauth/token"


# Abstract base class shared by all credential types supported by railib.
class Credentials(ABC):
    pass


# Represents access key credentials.
class AccessKeyCredentials(Credentials):
    def __init__(self, akey: str, pkey: str):
        self.akey = akey  # access_key
        self.pkey = pkey  # private_key


# Represents an OAuth access token.
class AccessToken:
    def __init__(self, access_token: str, expires_in: int):
        self.token = access_token
        self.expires_in = expires_in
        self.created_on = round(time.time())

    def is_expired(self):
        return time.time() - self.created_on >= self.expires_in


# Represents OAuth client credentials.
class ClientCredentials(Credentials):
    def __init__(self, client_id: str, client_secret: str,
                 client_credentials_url: str = None):
        self.access_token = None
        self.client_id = client_id
        self.client_secret = client_secret
        self.client_credentials_url = \
            client_credentials_url or DEFAULT_CLIENT_CREDENTIALS_URL
