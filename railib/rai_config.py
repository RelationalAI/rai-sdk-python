# RAIConfig - Class to represent RAIConfig
class RAICredentials:
    pass


class RAIConfig:
    def __init__(self, host: str, port: str, region: str, scheme: str,
                 credentials: RAICredentials, client_credentials_api_url: str):
        self.host = host
        self.port = port
        self.region = region
        self.scheme = scheme
        self.credentials = credentials
        self.client_credentials_api_url = client_credentials_api_url


# RAICredentials - Class to represent the RAICredentials
# It is an empty base class to represent generic credentials object for @Context.
class RAICredentials:
    def __init__(self):
        pass


# AccessKeyCredentials - Class to represent access key credentials, with access_key and private_key
class AccessKeyCredentials(RAICredentials):
    def __init__(self, akey: str, pkey: str):
        self.akey = akey
        self.pkey = pkey


# ClientCredentials - Class to represent client credentials, with client_id and client_credentials
class ClientCredentials(RAICredentials):
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
