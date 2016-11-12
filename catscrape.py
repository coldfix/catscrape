
import os
import sys
from itertools import islice

import asyncio
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urljoin

import bs4
import requests


POOL_SIZE = 30


def get(session, url, timeout=5.0, retries=2):
    for i in range(retries):
        try:
            return session.get(url, timeout=timeout)
        except requests.exceptions.Timeout:
            pass
    return session.get(url, timeout=timeout)


run = asyncio.ensure_future


class GetRequest:

    def __init__(self, url, callback, *args, **kwargs):
        self.url = url
        self.callback = callback
        self.args = args
        self.kwargs = kwargs
        self.running = False
        self.queue = None

    def start(self, queue):
        self.queue = queue
        self.running = True
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(None, get, queue.session, self.url)
        future.add_done_callback(self.on_complete)

    def on_complete(self, future):
        response = future.result()
        new_jobs = self.callback(response, *self.args, **self.kwargs)
        run(self.queue.substitute_job(self, new_jobs))
        self.queue = None


class JobQueue:

    def __init__(self, pool_size, session):
        self.pool_size = pool_size
        self.lock = asyncio.Lock()
        self.done = asyncio.Event()
        self.queue = []
        self.session = session

    def put(self, job, index=0):
        self.queue.insert(index, job)

    async def join(self):
        await self.lock.acquire()
        self._more()
        self.lock.release()
        await self.done.wait()

    def _more(self):
        for job in islice(self.queue, self.pool_size):
            if not job.running:
                job.start(self)
                # future = run(job)
                # future.add_done_callback(partial(self.substitute_job, job))
        if not self.queue:
            self.done.set()

    async def substitute_job(self, job, replace):
        await self.lock.acquire()
        index = self.queue.index(job)
        self.queue[index:index+1] = replace or ()
        self._more()
        self.lock.release()


def save_image(response, path):
    print(path)
    with open(path, 'wb') as f:
        f.write(response.content)


def get_fname(path, bigpic, title):
    return os.path.join(path, '{1} - {0}{2}'.format(
        title, *os.path.splitext(os.path.basename(
            urlparse(bigpic).path))))


def scrape(response, path):
    print(response.url)
    soup = bs4.BeautifulSoup(response.text, 'html.parser')

    # save album
    os.makedirs(path, exist_ok=True)
    jobs = [
        GetRequest(bigpic, save_image,
                   get_fname(path, bigpic, anchor['title']))
        for item in soup.find_all('div', 'thumb-container')
        for anchor in [item.find('a')]
        for thumb in [anchor.find('img')['src']]
        for bigpic in [thumb.replace('thumb-350-', '')]
    ]

    # save next pages
    next_page = soup.find('a', id='next_page')
    if next_page is not None and next_page['href'] != '#':
        page_url = urljoin(response.url, next_page['href'])
        jobs.append(GetRequest(page_url, scrape, path))
    return jobs


def main(url, path):
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(POOL_SIZE))
    with requests.Session() as session:
        queue = JobQueue(POOL_SIZE, session)
        queue.put(GetRequest(url, scrape, path))
        loop.run_until_complete(queue.join())


if __name__ == '__main__':
    main(*sys.argv[1:])
