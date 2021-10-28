# using OAuth client credentials
[default]
region = us-east
host = azure.relationalai.com
port = 443
client_id = <client-id>
client_secret = <client-secret>
client_credentials_url = https://login.relationalai.com/oauth/token

# using API access key (deprecated)
[default-accesskey]
region = us-east
host = azure.relationalai.com
port = 443
access_key = <your public access key>
private_key_filename = <name of file containing private key>

[local]
scheme = http
host = 127.0.0.1
port = 8010
access_key = <your public access key>
private_key_filename = <name of file containing private key>
