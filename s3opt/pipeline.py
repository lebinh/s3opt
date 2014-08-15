import re
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import threading
import logging

import boto

from clint.textui import progress

from s3opt.analyser import DecoratorAnalyser


__author__ = 'binhle'


class Pipeline(object):
    """
    A pipeline of multiple processors to process S3 objects.
    """

    def __init__(self, access_key=None, secret_key=None, dry_run=False, threads=None):
        self._pipeline = []
        self.access_key = access_key
        self.secret_key = secret_key
        self.dry_run = dry_run
        self.threads = threads
        self.pool = ThreadPool(threads)
        self.thread_local_buckets = threading.local()

    def append(self, analyser, pattern, ignore_case=True):
        if ignore_case:
            pattern = re.compile(pattern, flags=re.IGNORECASE)
        else:
            pattern = re.compile(pattern)
        self._pipeline.append((pattern, analyser))

    def analyse(self, pattern, ignore_case=True):
        def decorator(func):
            self.append(DecoratorAnalyser(func.__name__, func), pattern, ignore_case)
            return func

        return decorator

    def connect_s3(self):
        if self.access_key is not None and self.secret_key is not None:
            return boto.connect_s3(aws_access_key_id=self.access_key,
                                   aws_secret_access_key=self.secret_key)
        else:
            return boto.connect_s3()

    def get_bucket(self, name):
        if getattr(self.thread_local_buckets, name, None) is None:
            logging.info('Create new connection to S3 from thread %s', threading.currentThread())
            conn = self.connect_s3()
            bucket = conn.get_bucket(name)
            setattr(self.thread_local_buckets, name, bucket)
        return getattr(self.thread_local_buckets, name)

    def run(self, bucket, prefix='', show_progress=True):
        self.pre_run()

        bucket = self.get_bucket(bucket)
        keys = bucket.list(prefix)
        chunk_size = self.threads if self.threads is not None else cpu_count()
        it = self.pool.imap(self.analyse_key, keys, chunksize=chunk_size)
        if show_progress:
            list(progress.dots(it, label='Analysing bucket "%s"' % bucket.name))
        else:
            list(it)

        self.post_run()

    def pre_run(self):
        for _, analyser in self._pipeline:
            analyser.start()

    def post_run(self):
        for _, analyser in self._pipeline:
            analyser.finish()

    def analyse_key(self, key):
        bucket = self.get_bucket(key.bucket.name)
        for pattern, analyser in self._pipeline:
            if pattern.match(key.key):
                # update key metadata since last analyser might already modified it
                key = bucket.get_key(key.key)
                analyser.analyse(key, dry_run=self.dry_run)
