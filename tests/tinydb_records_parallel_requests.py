from werkzeug.wrappers import Request
from werkzeug.test import create_environ

from multiprocessing import Process

import pytest
from pygeoapi.api import (
    API, APIRequest
)
from pygeoapi.util import yaml_load

from .util import get_test_file_path


@pytest.fixture()
def config():
    with open(get_test_file_path('pygeoapi-test-config.yml')) as fh:
        return yaml_load(fh)


@pytest.fixture()
def api_(config):
    return API(config)


def _execute_process(api, request, process_id):
    return api.execute_process(request, process_id)


def _create_request(name, message, locales):

    data = {
        "mode": "async",
        "response": "raw",
        "inputs": {
            "name": name,
            "message": message
        }
    }
    environ = create_environ(base_url='http://localhost:5000/processes/hello-world/execution', method="POST", json=data)
    req = Request(environ)
    return APIRequest.with_data(req, locales)


def test_async_hello_world_process_parallel(api_):

    process_id = "hello-world"
    req1 = _create_request("World", "Hello", api_.locales)
    req2 = _create_request("Mars", "Hi", api_.locales)
    req3 = _create_request("Sun", "Greatings", api_.locales)

    # headers, http_status, response = _execute_process(api_, request, process_id)

    p1 = Process(target=_execute_process, args=(api_, req1, process_id))
    p2 = Process(target=_execute_process, args=(api_, req2, process_id))
    p3 = Process(target=_execute_process, args=(api_, req3, process_id))

    procs = []
    for p in [p1, p2, p3]:
        p.start()
        procs.append(p)
    for p in procs:
        p.join()


























