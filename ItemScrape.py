from bs4 import BeautifulSoup, Tag
from requests import Response
import requests
# from urllib.request import Request, urlopen, URLError, HTTPError
# from http.client import HTTPResponse
# import ssl, certifi, socket

from uuid import uuid4
from queue import Queue
import numpy as np

import concurrent.futures
import sys
import os
import time

class RequestScheduler():

    def __init__(self, request_queue: Queue, max_wait=90, max_concurrent=10, socket_timeout=25):
        super().__init__()
        self._request_q = request_queue
        self._max_wait = max_wait
        self._round_wait = 0.0
        self._max_concurrent = max_concurrent
        self._concurrent = 1
        self._socket_to = socket_timeout
        self._errorCount = 0
        self._start = time.time()
        self._check_dir()
    
    @classmethod
    def _check_dir(self):
        if 'data' in os.listdir():
            return
        os.mkdir('data')
        os.chdir('data')
    
    def run(self):
        while self._request_q.qsize() > 0:
            x = self._round()
            y = np.array(x)
            name = str(uuid4()) + '.npy'
            if y.size > 0:
                with open(name, 'wb') as f:
                    np.save(f, x)

    def getResponse(self, req: str) -> Response:
        try:
            #r = urlopen(req, context=ssl.create_default_context(cafile=certifi.where()), timeout=self._socket_to)
            r = requests.get(req, timeout=self._socket_to)
            self.adjustDelay(1)
            print(f'Response from {r.url} was {r}')
            self._request_q.task_done()
            return r
        except HTTPError as e:
            print(f'Data not retrieved because {e}\nURL:{req.full_url}', file=sys.stderr)
            self._errorCount += 1
            self._request_q.put(req)
            self.adjustDelay(-1)
        except ConnectionError as e:
            if isinstance(e.reason, socket.timeout):
                print(f'Socket Timeout - URL {req.full_url}\n', file=sys.stderr)
            else:
                print(f'Unknown Web Error\n {e}', file=sys.stderr)
            self._errorCount += 1
            self._request_q.put(req)
            self.adjustDelay(-1)         

    def adjustDelay(self, metric: int):
        if metric > 0: # no error increase sending speed
            self._concurrent += 1 if self._concurrent < self._max_concurrent else 0
            self._round_wait = (self._round_wait - 1) / 2 if self._round_wait >= 1 else 0
        if metric < 0: # error occurred back off on sending
            self._concurrent -= 1 if self._concurrent > 1 else 0
            self._round_wait = (self._round_wait + 1) * 2 if self._concurrent == 1 and self._round_wait < self._max_wait else self._round_wait + 1

    def send(self, reqs: list):
        print(f'\n\nprocessing requests {reqs}\n')
        with concurrent.futures.ThreadPoolExecutor() as executor:
            return executor.map(self.getResponse, reqs)

    def getRequests(self) -> list:
        r = []
        for _ in range(self._concurrent):
            if self._request_q.qsize() > 0:
                r.append(self._request_q.get())
        return r

    def _wait(self):
        time.sleep(self._round_wait)

    def _round(self):
        if (self._round_wait == self._max_wait):
            raise RuntimeError(f'Something is wrong, Max wait of {self._max_wait} reached.')
        
        self._wait()
        requests = self.getRequests()
        responses = list(self.send(requests))
        items = [x for x in responses if self.isValidItem(x)]
        return [self.trimData(x) for x in items]

    @staticmethod
    def isValidItem(page: Response):
        return page.text.find('<title>Not Found - Item - Classic wow database</title>') == -1

    @staticmethod
    def trimData(page: Response):
        id = page.url.replace('https://classicdb.ch/?item=','')
        itemDiv_ID = 'tooltip' + id + '-generic'
        soup = BeautifulSoup(page.text, 'html.parser')
        return soup.find(id=itemDiv_ID)
        

if __name__ == "__main__":
    urls = Queue()

    for i in [1,5,7,10,14400, 14555, 22438]:
        url = 'https://classicdb.ch/?item=' + str(i)
        urls.put(url)

    schedule = RequestScheduler(urls)
    schedule.run()

# EOF #
