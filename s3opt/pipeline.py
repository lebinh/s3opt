import re
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from clint.textui import progress

from s3opt.analyser import DecoratorAnalyser


__author__ = 'binhle'


class Pipeline(object):
    """
    A pipeline of multiple processors to process S3 objects.
    """

    def __init__(self, threads=None, dry_run=False):
        self._pipeline = []
        self.threads = threads
        self.pool = ThreadPool(threads)
        self.dry_run = dry_run

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

    def run(self, bucket, prefix='', show_progress=True):
        self.pre_run()

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
        for pattern, analyser in self._pipeline:
            if pattern.match(key.key):
                key = key.bucket.get_key(key.key)
                analyser.analyse(key, dry_run=self.dry_run)
