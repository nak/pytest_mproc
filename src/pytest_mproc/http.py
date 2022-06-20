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

from pytest_mproc.user_output import always_print
from aiohttp.web import Response


class HTTPSession:

    def __init__(self, session_id: str, end_url: str, start_url: str, heartbeat_url: str):
        self._end_url = end_url
        self._start_url = start_url
        self._hearbeat_task = asyncio.create_task(self._heartbeat(heartbeat_url))
        self._session_id = session_id

    @staticmethod
    def _check_http_status(resp: Union[Response, ClientResponse, requests.Response]) -> bool:
        if isinstance(resp, requests.Response):
            return resp.status_code >= 200 or resp.status_code <= 299
        else:
            return resp.status >= 200 or resp.status <= 299

    async def _heartbeat(self, hearbeat_url: str):
        async with aiohttp.ClientSession() as session:
            async with session.get(hearbeat_url) as resp:
                while True:
                    if resp.status >= 400 or resp.status < 500:
                        break
                    if not self._check_http_status(resp):
                        always_print(f"http call to {hearbeat_url} to provide heartbeat failed: {resp.reason}"
                                     f" [session_id: {self._session_id}")
                    await asyncio.sleep(1)

    async def start(self) -> AsyncGenerator[Dict[str, Any], Dict[str, Any]]:

        def validate(json_data: Dict[str, Any]):
            if type(json_data) != dict:
                raise ValueError(
                    f"Invalid json format for client host/args specification; expected dictionary"
                )
            if 'host' not in json_data:
                raise ValueError("'host' not found in provided json")
            if 'arguments' not in json_data:
                raise ValueError("'arguments' not found in provided json")
            if type(json_data['host']) != str:
                raise ValueError(
                    f"Invalid json format;  host is not a string name"
                )
            if type(json_data['arguments']) != dict:
                raise ValueError(
                    f"Invalid json format for client host/args specification; expected dictionary")
            for k, v in json_data['arguments'].items():
                if type(k) != str:
                    raise ValueError(
                        f"Invalid json format for client host/args specification; expected string for key")
                if type(v) not in (str, bool, int, float):
                    raise ValueError(
                        f"Invalid json format for client host/args specification; expected string for value")

        buffer = ""
        async with aiohttp.ClientSession() as session:
            async with session.get(self._start_url)as resp:
                if not self._check_http_status(resp):
                    raise SystemError(f"Request to {self._start_url} failed: {resp.reason}")
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
                        if buffer:
                            json_data = json.loads(buffer.strip())
                            validate(json_data)
                            for k, v in json_data['arguments'].items():
                                json_data['arguments'][k] = str(v)
                            yield json_data

    async def end_session(self):
        self._hearbeat_task.cancel()
        async with aiohttp.ClientSession() as session:
            async with session.get(self._end_url) as resp:
                if resp.status < 200 or resp.status > 299:
                    always_print(f"ALERT: Call to end session via {self._end_url} failed: {resp.reason}"
                                 f"[session_id: {self._session_id}")

    def end_session_sync(self):
        resp = requests.get(self._end_url)
        if not self._check_http_status(resp):
            always_print(f"ALERT: Call to end session via {self._end_url} failed: {resp.reason}"
                         f"[session_id: {self._session_id}")

    @classmethod
    def create(cls, http_uri) -> "HTTPSession":
        resp = requests.get(http_uri)
        if not cls._check_http_status(resp):
            raise Exception(f"Unable to start session from given URL {http_uri}")
        json_data = json.loads(resp.content)
        return HTTPSession(session_id=json_data['session_id'],
                           end_url=json_data['end_session_url'],
                           start_url=json_data['start_session_url'],
                           heartbeat_url=json_data.get('heartbeat_url'))




