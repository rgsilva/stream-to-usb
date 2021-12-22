#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from asyncio.streams import StreamReader, StreamWriter
import os
import sys
import time
import traceback
import fs

from bufferqueue import BufferQueue

# If we are running from the pyfuse3 source directory, try
# to load the module from there first.
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 'pyfuse3.pyx'))):
    sys.path.insert(0, os.path.join(basedir, 'src'))

from argparse import ArgumentParser
import asyncio
import stat
import logging
import errno
import pyfuse3
import pyfuse3_asyncio

import os

from dataclasses import dataclass
from typing import List

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

log = logging.getLogger(__name__)
pyfuse3_asyncio.enable()


@dataclass
class DiskOffsetRange:
    start: int
    end: int

    def is_within(self, value):
        return self.start <= value and value <= self.end


class TestFs(pyfuse3.Operations):
    def __init__(self, storage_file, monitored_ranges: List[DiskOffsetRange]):
        super(TestFs, self).__init__()

        self._silence = open("silence-100ms.mp3", "rb").read()

        storage_size = os.fstat(storage_file.fileno()).st_size
        self._inodes = {
            # pyfuse3.ROOT_INODE + 1: (b"test.mp3", 512 * 1024 * 1024, b"", 0o644),
            # pyfuse3.ROOT_INODE + 2: (b"pointer", 1, b"?", 0o777),
            pyfuse3.ROOT_INODE + 1: (b"storage", storage_size, None, 0o644),
        }

        self._queue: BufferQueue = BufferQueue()
        self._monitored_ranges = monitored_ranges
        self._storage_file = storage_file


    async def handle_socket(self, reader: StreamReader, writer: StreamWriter):
        while True:
            # TODO: maybe add a timeout?
            buffer = await reader.read(16384)
            logging.debug(f"socket read returned {len(buffer)} bytes")
            if len(buffer) == 0:
                break
            self._queue.put(buffer)
        writer.close()
        logging.debug("socket closed!")


    def _inode_by_name(self, needle):
        for (inode, (name, _, _, _)) in self._inodes.items():
            if name == needle:
                return inode
        return None


    async def getattr(self, inode, ctx=None):
        logging.debug(f"getattr, inode={inode}")
        entry = pyfuse3.EntryAttributes()

        if inode == pyfuse3.ROOT_INODE:
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_size = 0
        elif inode in self._inodes:
            entry.st_mode = (stat.S_IFREG | self._inodes[inode][3])
            entry.st_size = self._inodes[inode][1]
        else:
            raise pyfuse3.FUSEError(errno.ENOENT)

        stamp = int(1438467123.985654 * 1e9)
        entry.st_atime_ns = stamp
        entry.st_ctime_ns = stamp
        entry.st_mtime_ns = stamp
        entry.st_gid = os.getgid()
        entry.st_uid = os.getuid()
        entry.st_ino = inode

        return entry


    async def lookup(self, parent_inode, name, ctx=None):
        logging.debug(f"lookup, parent_inode={parent_inode}, name={name}")

        if parent_inode != pyfuse3.ROOT_INODE:
            raise pyfuse3.FUSEError(errno.ENOENT)

        inode = self._inode_by_name(name)
        logging.debug(inode)
        if inode is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        return await self.getattr(inode)


    async def opendir(self, inode, ctx):
        if inode != pyfuse3.ROOT_INODE:
            raise pyfuse3.FUSEError(errno.ENOENT)

        return inode

    async def readdir(self, fh, start_id, token):
        assert fh == pyfuse3.ROOT_INODE
        logging.debug(f"readdir, start_id={start_id}")

        if start_id >= len(self._inodes):
            return

        index = start_id
        for (inode, (name, _, _, _)) in self._inodes.items():
            logging.debug(f"readdir, reply: name={name}, inode={inode}")
            pyfuse3.readdir_reply(token, name, await self.getattr(inode), index + 1)
            index += 1
        return


    async def setxattr(self, inode, name, value, ctx):
        if inode != pyfuse3.ROOT_INODE or name != b'command':
            raise pyfuse3.FUSEError(errno.ENOTSUP)

        if value == b'terminate':
            pyfuse3.terminate()
        else:
            raise pyfuse3.FUSEError(errno.EINVAL)


    async def open(self, inode, flags, ctx):
        logging.debug(f"open, inode={inode}")
        if inode not in self._inodes:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # if flags & os.O_RDWR or flags & os.O_WRONLY:
        #     raise pyfuse3.FUSEError(errno.EACCES)

        info = pyfuse3.FileInfo(fh=inode)
        setattr(info, 'direct_io', True)
        return info


    def _is_monitored_offset(self, offset):
        for range in self._monitored_ranges:
            if range.is_within(offset):
                return True
        return False

    async def read(self, fh, off, size):
        assert fh in self._inodes
        #logging.debug(f"read, fh={fh}, off={off}, size={size}")

        is_monitored = self._is_monitored_offset(off)

        try:
            if is_monitored:
                # if self._queue.empty():
                #     # TODO: handle size < silence frames. probably with mod?
                #     logging.debug("returning silence")
                #     return self._silence[off:off+size] or self._silence[0:size]
                # else:
                #     buf = self._queue.get(size)
                #     logging.debug(f"returning queued {len(buf)} bytes, still {self._queue.size()} bytes queued")
                #     return buf

                #logging.debug("Monitored area, waiting for enough data!")
                await self._queue.wait_for(size)
                buf = self._queue.get(size)
                #logging.debug(f"returning queued {len(buf)} bytes, still {self._queue.size()} bytes queued")
                return buf

            self._storage_file.seek(off)
            return self._storage_file.read(size)
        except:
            logging.error(traceback.format_exc())

        return

    # async def write(self, fh, off, buf):
    #     assert fh in self._inodes
    #     logging.debug(f"write, fh={fh}, off={off}, buf={buf}")

    #     try:
    #         if self._inodes[fh][0] == b"pointer":
    #             data_index = int(buf[0]) - 48
    #             logging.debug("queueing data to index", data_index)
    #             self._queue.put(self._data[data_index])
    #     except Exception as e:
    #         logging.error(traceback.format_exc())

    #     return len(buf)

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    return parser.parse_args()


def parse_fat(disk_image_path: str, needle_file: str):
    DISK_OFFSET = 1048576
    my_fs = fs.open_fs(f"fat://{disk_image_path}?offset={DISK_OFFSET}")
    cluster_size = my_fs.fs.bytes_per_cluster
    (_, files, _) = my_fs.fs.root_dir.get_entries()

    result = []

    for file in files:
        if file.name.get_unpadded_filename().lower() == needle_file.lower():
            logging.info(f"FAT: found needle {needle_file}")
            first_cluster = file.fstcluslo

            for cluster in list(my_fs.fs.get_cluster_chain(first_cluster)):
                address = my_fs.fs.get_data_cluster_address(cluster) + DISK_OFFSET
                result.append(DiskOffsetRange(address, address + cluster_size))

    my_fs.close()

    return result or None
    
def simplified_ranges(original_ranges):
    ranges = sorted(original_ranges, key=lambda x:x.start)
    final_ranges = []

    current_start = ranges[0].start
    current_end = ranges[0].end
    for i in range(1, len(ranges)):
        cur = ranges[i]
        if cur.start == current_end:
            current_end = cur.end
        else:
            final_ranges.append(DiskOffsetRange(current_start, current_end))
            current_start = cur.start
            current_end = cur.end
    final_ranges.append(DiskOffsetRange(current_start, current_end))
    return final_ranges


def main():
    options = parse_args()
    init_logging(options.debug)

    monitored_ranges = parse_fat("mass_storage", "004.mp3")
    monitored_ranges = simplified_ranges(monitored_ranges)
    print("Monitoring ranges:", monitored_ranges)

    storage = open("mass_storage", "rb")
    testfs = TestFs(storage, monitored_ranges)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=hello_asyncio')

    if options.debug_fuse:
        fuse_options.add('debug')

    pyfuse3.init(testfs, options.mountpoint, fuse_options)
    loop = asyncio.get_event_loop()

    try:
        server = asyncio.start_server(lambda reader, writer: testfs.handle_socket(reader, writer), "0.0.0.0", 3123)
        loop.run_until_complete(server)
        loop.run_until_complete(pyfuse3.main())
    finally:
        loop.close()
        storage.close()
        pyfuse3.close(unmount=True)

if __name__ == '__main__':
    main()