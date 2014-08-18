"""s3opt

Usage:
    s3opt [options] <bucket/prefix> ...

Sample:
    s3opt --dry-run a-bucket another-bucket/prefix
    s3opt --image-max-age -1 text-max-age -1 bucket/no-cache/

Options:
    --access-key <access key>  AWS access key
    --secret-key <secret key>  AWS secret key
    -d, --dry-run
    -i, --image-max-age <seconds>  Cache-Control max-age value for image files (jpg/png/gif) [default: 604800]
    -t, --text-max-age <seconds>  Cache-Control max-age value for text files (html/css/js) [default: 86400]
    -p, --cache-private  Set Cache-Control: private instead of public
    --no-cache-control-check  Disable Cache-Control check and optimization
    --no-content-type-check  Disable Content-Type check and optimization
    -v, --verbose  Verbose logging
    --debug  Debug mode
"""
import logging

import docopt

from s3opt.pipeline import Pipeline
from s3opt.analyser import ContentTypeAnalyser, CacheControlAnalyser, JpegOptimiser, PngOptimiser


__author__ = 'binhle'


def init_pipeline(args):
    pipe = Pipeline(access_key=args['--access-key'], secret_key=args['--secret-key'], dry_run=args['--dry-run'])
    if not args['--no-content-type-check']:
        pipe.append(ContentTypeAnalyser('Content Type'), ".*")

    if not args['--no-cache-control-check']:
        image_max_age = int(args['--image-max-age'])
        text_max_age = int(args['--text-max-age'])
        extra = 'private' if args['--cache-private'] else 'public'
        if image_max_age >= 0:
            pipe.append(CacheControlAnalyser('Images Caching', image_max_age, extra=extra), '.*\.(jpe?g|png|gif)$')
        if text_max_age >= 0:
            pipe.append(CacheControlAnalyser('Text Caching', text_max_age, extra=extra), '.*\.(html?|css|js|json)$')

    pipe.append(JpegOptimiser('JPEG optimise'), '.*\.jpe?g$')
    pipe.append(PngOptimiser('PNG optimise'), '.*\.png$')

    return pipe


def select_targets(args):
    for bucket in args['<bucket/prefix>']:
        if '/' in bucket:
            bucket, prefix = bucket.split('/', 1)
        else:
            prefix = ''
        yield bucket, prefix


def main():
    args = docopt.docopt(__doc__)
    level = logging.INFO if args['--verbose'] else logging.WARNING
    if args['--debug']:
        level = logging.DEBUG
    logging.basicConfig(level=level, format='%(message)s')

    pipe = init_pipeline(args)
    targets = select_targets(args)
    show_progress = not args['--verbose']
    for bucket, prefix in targets:
        pipe.run(bucket, prefix=prefix, show_progress=show_progress)
