import json
import time

from urllib.request import Request, urlopen

from railib import utils
from railib.rai_config import RAIConfig

ACCESS_KEY_TOKEN_KEY = "access_token"
CLIENT_ID_KEY = "client_id"
CLIENT_SECRET_KEY = "client_secret"
AUDIENCE_KEY = "audience"
GRANT_TYPE_KEY = "grant_type"
CLIENT_CREDENTIALS_KEY = "client_credentials"
EXPIRES_IN_KEY = "expires_in"
CLIENT_CREDENTIALS_API_URL_PREFIX = "https://login"
CLIENT_CREDENTIALS_API_URL_POSTFIX = ".relationalai.com/oauth/token"
CLIENT_CREDENTIALS_API_SCHEME = "https://"
DEV_ENV_CHAR = "-"


# build_auth_token_api_url - Builds the url from audience/host field in the config.
# If the host has an environment like auzre-ux.relationalai.com, then it extracts the environment,
# and then build the url by concatenating CLIENT_CREDENTIALS_API_URL_PREFIX and CLIENT_CREDENTIALS_API_URL_POSTFIX
# like https://login-ux.relationalai.com/oauth/token
# If there is no environment then it would return the url like https://login.relationalai.com/oauth/token
def _build_client_credentials_api_url(audience: str):
    environment = None

    dev_env_index = audience.find(DEV_ENV_CHAR)
    if dev_env_index != -1:
        dot_index = audience.find(".", dev_env_index + 1)
        if dot_index != -1:
            environment = audience[dev_env_index + 1:-(len(audience) - dot_index)]
        else:
            environment = audience[dev_env_index + 1]

    if environment:
        return "{}{}{}{}".format(CLIENT_CREDENTIALS_API_URL_PREFIX, DEV_ENV_CHAR, environment,
                                 CLIENT_CREDENTIALS_API_URL_POSTFIX)
    else:
        return "{}{}".format(CLIENT_CREDENTIALS_API_URL_PREFIX, CLIENT_CREDENTIALS_API_URL_POSTFIX)


class ClientCredentialsService:
    def __init__(self, rai_config: RAIConfig):
        self.config = rai_config
        self._access_token = None

    def get_access_token(self):
        if self._access_token is None or self._access_token.is_expired():
            self._access_token = self._get_access_token()

        return self._access_token.access_token

    # get_auth_token - Gets the auth token from client credentials api service.
    def _get_access_token(self):
        # Normalize the audience or the host field to include the protocol scheme, like https.
        # If the protocol scheme is already there, then it would use the host as-is,
        # otherwise it will prepend the scheme, like https://auzre-ux.relationalai.com
        normalized_audience = self.config.host
        if not normalized_audience.startswith(CLIENT_CREDENTIALS_API_SCHEME):
            normalized_audience = "{}{}".format(CLIENT_CREDENTIALS_API_SCHEME, self.config.host)

        # create the payload for api call to get the client credentials (oauth token)
        body = {CLIENT_ID_KEY: self.config.credentials.client_id,
                CLIENT_SECRET_KEY: self.config.credentials.client_secret,
                AUDIENCE_KEY: normalized_audience,
                GRANT_TYPE_KEY: CLIENT_CREDENTIALS_KEY}
        data = utils.encode(body)

        # build the client credentials api url from host
        client_credentials_api_url = _build_client_credentials_api_url(self.config.host)

        headers = {}
        utils.default_headers(client_credentials_api_url, headers)

        # make POST call to the API to get an oauth token
        req = Request(method="POST", url=client_credentials_api_url, headers=headers, data=data)
        with urlopen(req) as rsp:
            result = json.loads(rsp.read())
            if result[ACCESS_KEY_TOKEN_KEY]:
                return AccessToken(result[ACCESS_KEY_TOKEN_KEY], result[EXPIRES_IN_KEY])

        raise Exception("failed to get the auth token")


class AccessToken:
    def __init__(self, access_token: str, expires_in: int):
        self.access_token = access_token
        self.expires_in = expires_in
        self.created_on = round(time.time())

    def is_expired(self):
        return time.time() - self.created_on >= self.expires_in
