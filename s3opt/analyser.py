import logging
import mimetypes

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
                self.log(colored.yellow('CHANGED: %d out of %d objects (%.2f) changed'), self.changed, self.total,
                         100.0 * self.changed / self.total)
            else:
                self.log(colored.red('PROBLEM: %d out of %d (%.2f %%) objects are problematic'),
                         self.problematic, self.total, 100.0 * self.problematic / self.total)
        else:
            self.log(colored.green('GOOD: all %d objects are ok'), self.total)

    def log(self, msg, *args):
        logging.warning('[%s] %s' % (self.name, msg), *args)


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
        if max_age < 0:
            self.cache_control += 'no-cache'
        else:
            self.cache_control += 'max-age=%d' % max_age

    def verify(self, key):
        if key.cache_control != self.cache_control:
            logging.info('Insufficient Cache-Control header set for %s: "%s", should be: "%s"',
                         key.key, key.cache_control, self.cache_control)
            return False
        return True

    def optimise(self, key):
        if key.get_redirect() is None:
            self.log('Changing Cache-Control header of "%s" to "%s"', key, self.cache_control)
            util.change_key_metadata(key, 'Cache-Control', self.cache_control)


class ContentTypeAnalyser(Analyser):
    """
    An analyser that verify and set the Content-Type header of objects.
    """

    def verify(self, key):
        content_type, _ = mimetypes.guess_type(key.key)
        if content_type is not None and content_type != key.content_type:
            logging.info('Incorrect Content-Type header set for %s: "%s", should be: "%s"',
                         key.key, key.content_type, content_type)
            return False
        return True

    def optimise(self, key):
        if key.get_redirect() is None:
            content_type, _ = mimetypes.guess_type(key.key)
            self.log('Changing Content-Type header of "%s" to be "%s"', key, content_type)
            util.change_key_metadata(key, 'Content-Type', content_type)
