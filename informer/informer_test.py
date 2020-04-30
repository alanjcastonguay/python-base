#!/usr/bin/env python

# Copyright 2016 The Kubernetes Authors.
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

import unittest
import time
from mock import Mock, call
import json
from .informer import Informer

from threading import Event

from kubernetes.client import V1Namespace, V1ObjectMeta


class InformerTests(unittest.TestCase):
    def setUp(self):
        # counter for a test that needs test global state
        self.callcount = 0

    def test_watch_with_decode(self):
        fake_resp = Mock()
        fake_resp.close = Mock()
        fake_resp.release_conn = Mock()

        expected_namespace_names = ('test1', 'test2', 'test3')

        block = Event()

        def list_namespace_chunked_mock(*args, **kwargs):
            print("list_namespace_list_or_watch_mock called", args, kwargs)

            for test_namespace in expected_namespace_names:
                event = {
                    "type": "ADDED",
                    "object": {
                        "api_version": "v1",
                        "kind": "Namespace",
                        "metadata": {
                            "name": test_namespace,
                            "selfLink": "/api/v1/namespaces/" + test_namespace,
                            "uid": test_namespace
                        }
                    }
                }
                body = json.dumps(event) + '\n'
                print("Dumping body", body)
                yield body

            # Hang the read
            time.sleep(1)
            if block.wait(timeout=10):
                raise ValueError("Timeout without clean closure")

        fake_resp.items = []  # Empty initial list
        fake_resp.read_chunked = Mock(side_effect=list_namespace_chunked_mock)

        fake_api = Mock()
        fake_api.list_namespace = Mock(return_value=fake_resp)
        fake_api.list_namespace.__doc__ = ':return: V1NamespaceList'  # The PYDOC_RETURN_LABEL used by

        w = Informer(fake_api.list_namespace)

        with w:
            time.sleep(1)  # let it prime asynchronously
            snapshot_items = w.items
            print("Done capturing items from informer", len(snapshot_items))

        # Stop hanging the read
        block.set()

        # Content
        assert all([_.kind == 'Namespace' for _ in snapshot_items])
        obtained_namespace_names = tuple([_.metadata.name for _ in snapshot_items])
        assert obtained_namespace_names == expected_namespace_names

        # Was the list_namespace() mock, aka list_namespace_chunked_mock, invoked with expected values?
        # Cannot use fake_api.list_namespace.assert_called_with(_preload_content=False, watch=True), as timeout_seconds and resource_version may also be passed
        call_args, call_kwargs = fake_api.list_namespace.call_args
        assert call_kwargs['watch'] is True
        assert call_kwargs['_preload_content'] is False

        fake_resp.read_chunked.assert_called_once_with(decode_content=False)
        fake_resp.close.assert_called()
        fake_resp.release_conn.assert_called_once()


if __name__ == '__main__':
    unittest.main()
