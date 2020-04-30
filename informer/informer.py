#!/usr/bin/env python

# Copyright 2019 The Kubernetes Authors.
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

"""
Populate a local cache mirroring a remote collection of resources.
Leverages a list_* function for initial sync and subscribing with Watch.
Modeled after client-go's Informer and Cache.
https://github.com/kubernetes/client-go/blob/master/tools/cache/listwatch.go
"""

from copy import deepcopy
import logging
from typing import Union, Callable, List, Type, Optional
from threading import Event, Thread, Lock

from datetime import datetime, timedelta

from kubernetes.watch.watch import Watch
from kubernetes.client import V1ObjectMeta, ApiClient, V1ObjectReference

from urllib3.exceptions import ReadTimeoutError
from multiprocessing import Process


logger = logging.getLogger(__name__)


class EventType(object):
    """
    Constants defined in k8s.io/apimachinery/pkg/watch
    """
    added = 'ADDED'
    modified = 'MODIFIED'
    deleted = 'DELETED'
    error = 'ERROR'


class Informer(Thread):

    _items = {}
    should_run = True

    def __init__(
            self,
            list_function: Callable,
            added: Optional[Event] = None,  # these are optional callbacks
            modified: Optional[Event] = None,
            deleted: Optional[Event] = None,
            shutdown_signal: Optional[Event] = None,
            return_type: Optional[Type] = None,
            api_client: Optional[ApiClient] = None,
            relist_period=timedelta(seconds=360),  # Short client timeout
            timeout_seconds=timedelta(hours=1),  # Long server timeout, >=1h
            **kwargs
    ):
        """
        :param kube_client:
        :param list_function: A callback function like kubernetes.client.CoreV1Api(api_client).list_*() which will be called repeatedly.
        :param kwargs: Extra arguments that will be passed to the list_function
        :param relist_period: Every period, drop the watcher and request a full list.
        """
        self.list_function = list_function
        self.callback_handlers = {
            EventType.added: added,
            EventType.modified: modified,
            EventType.deleted: deleted,
        }
        self.shutdown_signal = shutdown_signal
        self.return_type = return_type
        self.api_client = api_client
        self._client_timeout_seconds = int(relist_period.total_seconds())
        self._server_timeout_seconds = int(timeout_seconds.total_seconds())
        self.watcher = None

        # Arbitrary extra arguments will be passed to the list function.
        self.kwargs = kwargs

        self._items = {}
        self._item_lock = Lock()

        self._heartbeat = datetime.utcnow()

        # Name the Thread after the list function.
        super(Informer, self).__init__(name=str(list_function), daemon=True)

    @property
    def items(self):
        """
        A list of all items currently in the informer cache, guarded by a mutex.
        The items may be of any type, influenced by :self.list_function:
        Items are guaranteed to have a metadata property of type V1ObjectMeta.
        :return: List[]
        """
        with self._item_lock:
            return deepcopy(list(self._items.values()))

    @property
    def age(self):
        """
        How long ago did the informer last get an update from the watch stream? May be used for checking health of the informer.
        :return: timedelta
        """
        return datetime.utcnow() - self._heartbeat

    @property
    def item_names(self):
        """
        A list of the names
        :return: List[str]
        """
        return [o.metadata.name for o in self.items]

    def _run_once(self):
        """
        Fetch all resources, and subscribe for changes until timeout is reached.
        :return: None
        """
        logger.debug("Full relist of resources")
        initial_list = self.list_function(
            timeout_seconds=self._server_timeout_seconds,  # Server timeout
            _request_timeout=self._client_timeout_seconds,  # Client timeout
            **self.kwargs
        )

        resource_version = initial_list.metadata.resource_version

        with self._item_lock:
            self._items.clear()
            for obj in initial_list.items:
                self._items[key_func(obj)] = obj

        self._heartbeat = datetime.utcnow()

        # Stream updates until timeout is reached, starting from resource_version

        self.watcher = Watch(self.return_type)

        stream = self.watcher.stream(
            self.list_function,
            resource_version=resource_version,
            timeout_seconds=self._server_timeout_seconds,
            _request_timeout=self._client_timeout_seconds,
            **self.kwargs
        )

        for ev in stream:
            self._heartbeat = datetime.utcnow()

            obj = ev["object"]
            operation = ev['type']

            try:
                # Object is expected to have a metadata section, holding a V1ObjectMeta.
                # No other guarantees are made about its type.
                metadata = obj.metadata
            except AttributeError:
                logger.exception("Cannot process event for unknown object", extra={"event": ev})
                continue

            # Update with the new most-recently-seen resource version. This is a monotonic-increasing integer in a string field.
            # The next iteration will resume from this offset.
            resource_version = metadata.resource_version

            # Update the cache
            if operation in (EventType.added, EventType.modified):
                with self._item_lock:
                    self._items[key_func(obj)] = obj
            elif operation == EventType.deleted:
                with self._item_lock:
                    del self._items[key_func(obj)]
            elif operation == EventType.error:
                raise ValueError("Event type %s received" % EventType.error, ev)
            else:
                raise KeyError("Unknown event type %s received" % operation, ev)

            try:
                self.callback_handlers[operation].set()
            except (KeyError, AttributeError):
                pass  # No handler defined

    def run(self):
        self.should_run = True
        while self.should_run:
            try:
                self._run_once()
            except AttributeError:  # http.client.HTTPResponse._safe_read() tries to call self.fp.read() while self.fp is None after connection is closed
                pass
            except ReadTimeoutError:  # Client-side timeout. Loop and reconnect as long as self.should_run
                pass
            #except Exception:
            #    if self.should_run:
            #        raise
            #except AttributeError as e:
            #    print("Attribute error on the socket", e, "while should_run", self.should_run)
            ##   File "/usr/local/Cellar/python/3.7.4/Frameworks/Python.framework/Versions/3.7/lib/python3.7/http/client.py", line 620, in _safe_read
            ##     chunk = self.fp.read(min(amt, MAXAMOUNT))
            ## AttributeError: 'NoneType' object has no attribute 'read'
            #
            #except Exception:  # TODO: This is too broad.
            #    raise
            #    pass

        # The only routes here are stop() and should_run=False
        # If the shutdown_signal Event object is defined, fire it.
        if self.shutdown_signal is not None:
            self.shutdown_signal.set()

    def stop(self):
        self.should_run = False

        #self.terminate()
        try:
            self.watcher.stop()
        except AttributeError:
            pass


        import time
        time.sleep(1)
        self.join(int(self._client_timeout_seconds) + 1)
        if self.is_alive():
            print(self, self.watcher)
            raise RuntimeError("Informer thread did not gracefully terminate.")

    def start(self):
        """
        Start the thread. The cache at .items will be populated asynchronously.
        :return:
        """
        super(Informer, self).start()

    # Usable as a context manager. Valuable when starting and stopping many instances, to ensure they don't leak.
    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Wait self.relist_period for the watching thread to self-terminate. If it doesn't, raise a RuntimeError.
        """
        self.stop()


def key_func(obj) -> str:
    """
    Generate a key usable for store hashmap.
    :param obj: An object with an attribute 'metadata' of type V1ObjectMeta
    :return: str: A key of the format <namespace>/<name> unless <namespace> is empty, then it's just <name>.
    """
    # https://github.com/kubernetes/client-go/blob/b560730196913ed063d37fbbe88f0979614029c5/tools/cache/store.go#L76
    meta = obj.metadata  # type: V1ObjectMeta
    if meta.namespace:
        return "%s/%s" % (meta.namespace, meta.name)
    return meta.name
