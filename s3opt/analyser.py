from __future__ import division
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
                self.warn('CHANGED: %d out of %d objects changed (%.2f%%).',
                          self.changed, self.total, 100 * self.changed / self.total)
            else:
                self.error('PROBLEM: %d out of %d objects are problematic (%.2f%%).',
                           self.problematic, self.total, 100 * self.problematic / self.total)
        else:
            self.good('GOOD: all %d objects are ok.', self.total)

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


class ContentOptimiser(Analyser):
    def analyse(self, key, dry_run=False):
        self.total += 1
        original_content = util.get_key_content(key)
        if not original_content:
            self.warn('Empty object: "%s"' % key.key)
            return

        optimised_content = self.optimise_content(key, original_content)
        if optimised_content and not self.verify_content(key, original_content, optimised_content):
            self.problematic += 1
            if not dry_run:
                self.warn('Changing content of "%s" to optimised version' % key)
                util.set_key_content(key, optimised_content)
                self.changed += 1

    def optimise_content(self, key, content):
        raise NotImplementedError

    def verify_content(self, key, original_content, optimised_content):
        raise NotImplementedError


class ContentSizeOptimiser(ContentOptimiser):
    min_compression_save = 1000
    min_compression_save_percentage = 10

    _total_size = 0
    _total_saved = 0

    def verify_content(self, key, original_content, optimised_content):
        self._total_size += len(original_content)
        save = len(original_content) - len(optimised_content)
        save_percentage = save / len(original_content) * 100
        if save > self.min_compression_save or save_percentage > self.min_compression_save_percentage:
            self.info('Optimise "%s" could save %s (%.2f%% reduction)', key.key, util.humanize(save),
                      save_percentage)
            self._total_saved += save
            return False
        return True

    def finish(self):
        if self.problematic > 0:
            if self.changed > 0:
                self.warn('CHANGED: %d out of %d objects changed, saved %s (%.2f%% reduction).',
                          self.changed, self.total, util.humanize(self._total_saved),
                          self._total_saved / self._total_size * 100)
            else:
                self.error('PROBLEM: %d out of %d objects can be optimized to save %s (%.2f%% reduction).',
                           self.problematic, self.total, util.humanize(self._total_saved),
                           self._total_saved / self._total_size * 100)
        else:
            self.good('GOOD: all %d objects are ok.', self.total)


class JpegOptimiser(ContentSizeOptimiser):
    def __init__(self, name, max_quality=100):
        super(JpegOptimiser, self).__init__(name)
        self.max_quality = max_quality

    def optimise_content(self, key, content):
        cmd_args = ['jpegoptim', '--quiet', '--strip-all', '--all-progressive']
        if self.max_quality < 100:
            # lossy compress image
            cmd_args.append('--max=%d' % self.max_quality)
        return util.optimise_external(content, cmd_args, temp_file_suffix='.jpg')


class PngOptimiser(ContentSizeOptimiser):
    def optimise_content(self, key, content):
        cmd_args = ['optipng', '-quiet', '-strip', 'all']
        return util.optimise_external(content, cmd_args, temp_file_suffix='.png')


class GzipAnalyser(ContentSizeOptimiser):
    def analyse(self, key, dry_run=False):
        self.total += 1
        if key.content_encoding == 'gzip':
            return

        original_content = util.get_key_content(key)
        if not original_content:
            self.warn('Empty object: "%s"' % key.key)
            return

        optimised_content = util.gzip(original_content)
        if optimised_content and not self.verify_content(key, original_content, optimised_content):
            self.problematic += 1
            if not dry_run:
                self.warn('Gzip content of "%s"' % key)
                # set content_encoding to 'gzip' and util.set_key_content with gzip the original content automatically
                key.content_encoding = 'gzip'
                util.set_key_content(key, original_content)
                self.changed += 1

