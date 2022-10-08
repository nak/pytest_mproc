"""
This module handles an optional connection to a cloud to establish a session for reserving and
relinquishing resources at beginning/end of testing
"""
import asyncio
import json
from typing import Dict, Any, AsyncGenerator, Union

import aiohttp
import requests
from aiohttp import ClientResponse

from pytest_mproc.user_output import always_print, debug_print
from aiohttp.web import Response


class HTTPSession:

    def __init__(self, session_id: str, end_url: str, start_url: str, heartbeat_url: str):
        self._end_url = end_url
        self._start_url = start_url
        self._heartbeat_task = asyncio.create_task(self._heartbeat(heartbeat_url))
        self._session_id = session_id

    @staticmethod
    def _check_http_status(resp: Union[Response, ClientResponse, requests.Response]) -> bool:
        if isinstance(resp, requests.Response):
            return resp.status_code >= 200 or resp.status_code <= 299
        else:
            return resp.status >= 200 or resp.status <= 299

    async def _heartbeat(self, heartbeat_url: str):
        async with aiohttp.ClientSession() as session:
            while True:
                async with session.get(heartbeat_url) as resp:
                    if resp.status == 404:
                        always_print(f"Session no longer found.  Terminating heartbeat")
                        break
                    if not self._check_http_status(resp):
                        always_print(f"http call to {heartbeat_url} to provide heartbeat failed: {resp.reason}"
                                     f" [session_id: {self._session_id}")
                        break
                    await asyncio.sleep(1)

    async def start(self) -> AsyncGenerator[Dict[str, Any], Dict[str, Any]]:

        def validate(json_data_: Dict[str, Any]):
            if type(json_data_) != dict:
                raise ValueError(
                    f"Invalid json format for client host/args specification; expected dictionary"
                )
            if 'host' not in json_data_:
                raise ValueError("'host' not found in provided json")
            if 'arguments' not in json_data_:
                raise ValueError("'arguments' not found in provided json")
            if type(json_data_['host']) != str:
                raise ValueError(
                    f"Invalid json format;  host is not a string name"
                )
            if type(json_data_['arguments']) != dict:
                raise ValueError(
                    f"Invalid json format for client host/args specification; expected dictionary")
            for key, val in json_data_['arguments'].items():
                if type(key) != str:
                    raise ValueError(
                        f"Invalid json format for client host/args specification; expected string for key")
                if type(val) not in (str, bool, int, float):
                    raise ValueError(
                        f"Invalid json format for client host/args specification; expected string for value")

        buffer = ""
        timeout = aiohttp.ClientTimeout(sock_read=60*60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self._start_url)as resp:
                if not self._check_http_status(resp):
                    raise SystemError(f"Request to {self._start_url} failed: {resp.reason}")
                debug_print("Call to start session successful, awaiting devices to be preped and ready...")
                try:
                    async for data, _ in resp.content.iter_chunks():
                        data = data.decode('utf-8')
                        buffer += data
                        if '\n' in data:
                            *lines, buffer = data.split('\n')
                            for line in lines:
                                if not line.strip():
                                    continue
                                json_data = json.loads(line.strip())
                                validate(json_data)
                                for k, v in json_data['arguments'].items():
                                    json_data['arguments'][k] = str(v)
                                yield json_data
                                debug_print(f"Using worker {json_data}")
                            if buffer:
                                json_data = json.loads(buffer.strip())
                                validate(json_data)
                                for k, v in json_data['arguments'].items():
                                    json_data['arguments'][k] = str(v)
                                yield json_data
                                debug_print(f"Using worker {json_data}")
                except asyncio.TimeoutError:
                    always_print("Timeout waiting for a worker", as_error=True)
                    raise
                except Exception as e:
                    always_print(f"Error getting any worker: {e} [{type(e)}]", as_error=True)
                    raise
                finally:
                    always_print(f"Got all workers from {self._start_url}")

    async def end_session(self):
        self._heartbeat_task.cancel()
        async with aiohttp.ClientSession() as session:
            async with session.get(self._end_url) as resp:
                if resp.status < 200 or resp.status > 299:
                    always_print(f"ALERT: Call to end session via {self._end_url} failed: {resp.reason}"
                                 f"[session_id: {self._session_id}")

    def end_session_sync(self):
        self._heartbeat_task.cancel()
        resp = requests.get(self._end_url)
        if not self._check_http_status(resp):
            always_print(f"ALERT: Call to end session via {self._end_url} failed: {resp.reason}"
                         f"[session_id: {self._session_id}", as_error=True)
        else:
            always_print(f"Called end session {self._end_url}")

    @classmethod
    def create(cls, http_uri) -> "HTTPSession":
        resp = requests.get(http_uri)
        if not cls._check_http_status(resp):
            raise Exception(f"Unable to create session from given URL {http_uri}")
        resp.raise_for_status()
        json_data = json.loads(resp.content)
        return HTTPSession(session_id=json_data['session_id'],
                           end_url=json_data['end_session_url'],
                           start_url=json_data['start_session_url'],
                           heartbeat_url=json_data.get('heartbeat_url'))
