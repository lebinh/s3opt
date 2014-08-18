import os
import subprocess
from subprocess import CalledProcessError
from tempfile import NamedTemporaryFile
import logging

__author__ = 'binhle'


def change_key_metadata(key, meta_key, meta_value):
    meta = key.metadata
    if key.cache_control:
        meta['Cache-Control'] = key.cache_control
    if key.content_type:
        meta['Content-Type'] = key.content_type
    if key.content_encoding:
        meta['Content-Encoding'] = key.content_encoding
    if key.content_disposition:
        meta['Content-Disposition'] = key.content_disposition
    if key.content_language:
        meta['Content-Language'] = key.content_language
    meta[meta_key] = meta_value
    return key.copy(key.bucket.name, key.key, metadata=meta, preserve_acl=True)


def optimise_external(content, cmd_args, temp_file_suffix):
    with NamedTemporaryFile(suffix=temp_file_suffix, delete=False) as tmp:
        tmp.write(content)
        tmp.flush()
        os.fsync(tmp)

        args = list(cmd_args)
        args.append(tmp.name)
        try:
            subprocess.check_call(args)
        except CalledProcessError:
            logging.exception('Error when running external optimiser command')
            return content
        with open(tmp.name, 'rb') as result:
            return result.read()


def humanize(bytes):
    if bytes < 1000:
        return '%dB' % bytes
    if bytes < 1000000:
        return '%.2fKB' % (bytes / 1000)
    return '%.2fMB' % (bytes / 1000000)
