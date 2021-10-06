# RAIConfig - Class to represent RAIConfig
class RAIConfig:
    def __init__(self, host, port, region, scheme, credentials):
        self.host = host
        self.port = port
        self.region = region
        self.scheme = scheme
        self.credentials = credentials


# RAICredentials - Class to represent the RAICredentials
# It is an empty base class to represent generic credentials object for @Context.
class RAICredentials:
    def __init__(self):
        pass


# AccessKeyCredentials - Class to represent access key credentials, with access_key and private_key
class AccessKeyCredentials(RAICredentials):
    def __init__(self, akey, pkey):
        self.akey = akey
        self.pkey = pkey


# ClientCredentials - Class to represent client credentials, with client_id and client_credentials
class ClientCredentials(RAICredentials):
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
