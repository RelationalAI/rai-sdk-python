from urllib.request import HTTPError

import json

# TODO: update show.http_error?

def show_error(e: HTTPError) -> None:
    r = e.read()
    if len(r) > 0:
        rsp = json.loads(r)
        print(f"Got error, status: {e.status}")
        print(json.dumps(rsp, indent=2))
    else:
        print("There is no descriptive error message, got:", e)


def wrap_error(fun, *args, **kwargs):
    try:
        fun(*args, **kwargs)
    except HTTPError as e:
        show_error(e) # vs. show.http_error(e)?

