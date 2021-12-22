import asyncio
from threading import Lock, Thread
from asyncio import Future
from time import sleep

class BufferQueue:
    def __init__(self):
        self._buffer: bytes = bytes()
        self._lock = Lock()
    

    def put(self, bytes: bytes):
        self._lock.acquire()
        self._buffer += bytes
        self._lock.release()


    def get(self, size: int) -> bytes:
        self._lock.acquire()
        buf = self._buffer[0:size]
        self._buffer = self._buffer[size:]
        self._lock.release()
        return buf


    def empty(self) -> bool:
        self._lock.acquire()
        empty = len(self._buffer) == 0
        self._lock.release()
        return empty


    def size(self) -> int:
        self._lock.acquire()
        val = len(self._buffer)
        self._lock.release()
        return val


    def wait_for(self, size: int) -> Future:
        future = Future()
        if self.size() >= size:
            future.set_result(True)
        else:
            Thread(target=self._wait_for_loop, args=(size, future,)).start()
        return future

    def _wait_for_loop(self, size, future):
        while True:
            if self.size() >= size:
                future.set_result(True)
                break
            sleep(0.25)
