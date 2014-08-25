from __future__ import division
from gzip import GzipFile
import os
import subprocess
from subprocess import CalledProcessError
from tempfile import NamedTemporaryFile
import logging

from cStringIO import StringIO


__author__ = 'binhle'


def get_all_metadata(key):
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
    return meta


def change_key_metadata(key, meta_key, meta_value):
    meta = get_all_metadata(key).copy()
    meta[meta_key] = meta_value
    return key.copy(key.bucket.name, key.key, metadata=meta, preserve_acl=True)


def get_key_content(key):
    """
    Get content of given key, auto unzip on the fly if its Content-Encoding is 'gzip'.
    """
    content = key.get_contents_as_string()
    if key.content_encoding == 'gzip':
        sio = StringIO(content)
        with GzipFile(fileobj=sio) as gzip:
            return gzip.read()
    return content


def set_key_content(key, content):
    """
    Set new content for given key and preserve its metadata and acl.
    Gzip the content on the fly if Content-Encoding of given key is 'gzip'.
    """
    acl_xml = key.get_xml_acl()
    metadata = get_all_metadata(key)
    if key.content_encoding == 'gzip':
        sio = StringIO()
        with GzipFile(fileobj=sio, mode='wb') as gzip:
            gzip.write(content)
        content = sio.getvalue()
    key.set_contents_from_string(content, headers=metadata)
    key.set_xml_acl(acl_xml)


def optimise_external(content, cmd_args, temp_file_suffix=""):
    """
    Optimise content by running an external command.
    """
    with NamedTemporaryFile(suffix=temp_file_suffix) as tmp:
        tmp.write(content)
        tmp.flush()
        os.fsync(tmp)

        args = list(cmd_args)
        args.append(tmp.name)
        try:
            subprocess.check_call(args)
        except CalledProcessError:
            logging.exception('Error when running external optimiser command')
            return None
        with open(tmp.name, 'rb') as result:
            return result.read()


def humanize(bytes):
    if bytes < 1000:
        return '%dB' % bytes
    if bytes < 1000000:
        return '%.2fKB' % (bytes / 1000)
    return '%.2fMB' % (bytes / 1000000)
