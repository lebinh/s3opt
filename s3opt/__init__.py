"""s3opt

Usage:
    s3opt [options] <bucket/prefix> ...

Sample:
    s3opt --dry-run a-bucket another-bucket/prefix
    s3opt --image-max-age -1 text-max-age -1 bucket/no-cache/

Options:
    --access-key <access key>  AWS access key
    --secret-key <secret key>  AWS secret key
    -d, --dry-run  Only check and print result without modify anything
    -v, --verbose  Verbose logging
    --debug  Debug mode

Content-Type header checking
    --no-content-type-check  Disable Content-Type check and optimization

Caching options:
    --no-cache-control-check  Disable Cache-Control check and optimization
    -i, --image-max-age <seconds>  Cache-Control max-age value for image files (jpg/png/gif) [default: 604800]
    -t, --text-max-age <seconds>  Cache-Control max-age value for text files (html/css/js) [default: 604800]
    -p, --cache-private  Set Cache-Control: private instead of public

Image compression options:
    --no-optimise-image  Disable optimising JPEG and PNG images
    -m, --max-quality <quality>  Set max quality used for compress JPEG images (< 100 means lossy compression)
                                [default: 100]

Gzip compression options:
    --gzip  Enable gzip of text content
"""
import logging

import docopt

from s3opt.pipeline import Pipeline
from s3opt.analyser import ContentTypeAnalyser, CacheControlAnalyser, JpegOptimiser, PngOptimiser, GzipAnalyser


__author__ = 'binhle'


def init_pipeline(args):
    pipe = Pipeline(access_key=args['--access-key'], secret_key=args['--secret-key'], dry_run=args['--dry-run'])

    if not args['--no-optimise-image']:
        max_quality = int(args['--max-quality'])
        pipe.append(JpegOptimiser('JPEG optimise', max_quality=max_quality), '.*\.jpe?g$')
        pipe.append(PngOptimiser('PNG optimise'), '.*\.png$')

    if args['--gzip']:
        pipe.append(GzipAnalyser('Gzip'), '.*\.(html?|css|js)$')

    if not args['--no-content-type-check']:
        pipe.append(ContentTypeAnalyser('Content Type'), ".*")

    if not args['--no-cache-control-check']:
        image_max_age = int(args['--image-max-age'])
        text_max_age = int(args['--text-max-age'])
        extra = 'private' if args['--cache-private'] else 'public'
        if image_max_age >= 0:
            pipe.append(CacheControlAnalyser('Images Caching', image_max_age, extra=extra), '.*\.(jpe?g|png|gif)$')
        if text_max_age >= 0:
            pipe.append(CacheControlAnalyser('Text Caching', text_max_age, extra=extra), '.*\.(html?|css|js)$')

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
