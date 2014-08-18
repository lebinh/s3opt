from __future__ import division
import logging
import mimetypes
from gzip import GzipFile

from cStringIO import StringIO

from clint.textui import colored

from s3opt import util


__author__ = 'binhle'


class Analyser(object):
    """
    Base class for all analyser
    """

    def __init__(self, name):
        self.name = name
        self.total = 0
        self.problematic = 0
        self.changed = 0

    def analyse(self, key, dry_run=False):
        self.total += 1
        if not self.verify(key):
            self.problematic += 1
            if not dry_run:
                self.optimise(key)
                self.changed += 1

    def verify(self, key):
        raise NotImplementedError

    def optimise(self, key):
        raise NotImplementedError

    def start(self):
        self.total = 0
        self.problematic = 0
        self.changed = 0

    def finish(self):
        if self.problematic > 0:
            if self.changed > 0:
                self.warn('CHANGED: %d out of %d objects (%.2f) changed', self.changed, self.total,
                          100 * self.changed / self.total)
            else:
                self.error('PROBLEM: %d out of %d (%.2f %%) objects are problematic',
                           self.problematic, self.total, 100 * self.problematic / self.total)
        else:
            self.good('GOOD: all %d objects are ok', self.total)

    def info(self, msg, *args):
        logging.info('[%s] %s' % (self.name, msg), *args)

    def warn(self, msg, *args):
        logging.warning(colored.yellow('[%s] %s' % (self.name, msg)), *args)

    def error(self, msg, *args):
        logging.warning(colored.red('[%s] %s' % (self.name, msg)), *args)

    def good(self, msg, *args):
        logging.warning(colored.green('[%s] %s' % (self.name, msg)), *args)


class DecoratorAnalyser(Analyser):
    """
    A class to wrap functional analyser using @pipeline.analyse decorator.
    """

    def __init__(self, name, decorated_func):
        super(DecoratorAnalyser, self).__init__(name)
        self.decorated_func = decorated_func

    def analyse(self, key, dry_run=False):
        if not dry_run:
            self.decorated_func(key)

    def finish(self):
        pass


class CacheControlAnalyser(Analyser):
    """
    An analyser that verify and set the Cache-Control header of objects.
    """

    def __init__(self, name, max_age, extra=None):
        super(CacheControlAnalyser, self).__init__(name)

        self.cache_control = '%s, ' % extra if extra is not None else ''
        if max_age <= 0:
            self.cache_control += 'no-cache'
        else:
            self.cache_control += 'max-age=%d' % max_age

    def verify(self, key):
        if key.cache_control != self.cache_control:
            self.info('Cache-Control header of "%s" should be "%s" instead of "%s"',
                      key, self.cache_control, key.cache_control)
            return False
        return True

    def optimise(self, key):
        self.warn('Changing Cache-Control header of "%s" to "%s"', key, self.cache_control)
        util.change_key_metadata(key, 'Cache-Control', self.cache_control)


class ContentTypeAnalyser(Analyser):
    """
    An analyser that verify and set the Content-Type header of objects.
    """

    def verify(self, key):
        content_type, _ = mimetypes.guess_type(key.key)
        if content_type is not None and content_type != key.content_type:
            self.info('Content-Type header of "%s" should be "%s" instead of "%s"',
                      key, content_type, key.content_type)
            return False
        return True

    def optimise(self, key):
        content_type, _ = mimetypes.guess_type(key.key)
        self.warn('Changing Content-Type header of "%s" to "%s"', key, content_type)
        util.change_key_metadata(key, 'Content-Type', content_type)


def get_key_content(key):
    content = key.get_contents_as_string()
    if key.content_encoding == 'gzip':
        sio = StringIO(content)
        with GzipFile(fileobj=sio) as gzip:
            return gzip.read()
    return content


def set_key_content(key, content):
    if key.content_encoding == 'gzip':
        sio = StringIO(content)
        with GzipFile(fileobj=sio) as gzip:
            return key.set_contents_from_string(gzip.read())
    return key.set_contents_from_string(content)


class ContentOptimiser(Analyser):
    def analyse(self, key, dry_run=False):
        self.total += 1
        original_content = get_key_content(key)
        optimised_content = self.optimise_content(key, original_content)
        if not self.verify_content(key, original_content, optimised_content):
            self.problematic += 1
            if not dry_run:
                self.warn('Changing content of "%s" to optimised version' % key)
                set_key_content(key, optimised_content)
                self.changed += 1

    def optimise_content(self, key, content):
        raise NotImplementedError

    def verify_content(self, key, original_content, optimised_content):
        raise NotImplementedError


class ExternalCommandOptimiser(ContentOptimiser):
    external_cmd_args = ['jpegoptim', '--strip-all', '--quiet', '--all-progressive']
    min_compression_save = 1000
    min_compression_save_percentage = 10
    temp_file_suffix = ''

    def optimise_content(self, key, content):
        return util.optimise_external(content, self.external_cmd_args, temp_file_suffix=self.temp_file_suffix)

    def verify_content(self, key, original_content, optimised_content):
        save = len(original_content) - len(optimised_content)
        save_percentage = save / len(original_content) * 100
        if save > self.min_compression_save or save_percentage > self.min_compression_save_percentage:
            self.info('Optimise "%s" could save %s (%.2f %%)', key.key, util.humanize(save),
                      save_percentage)
            return False
        return True


class JpegOptimiser(ExternalCommandOptimiser):
    external_cmd_args = ['jpegoptim', '--quiet', '--strip-all', '--all-progressive']
    temp_file_suffix = '.jpg'


class PngOptimiser(ExternalCommandOptimiser):
    external_cmd_args = ['optipng', '--quiet', '-strip', 'all', '-o6']
    temp_file_suffix = '.png'
