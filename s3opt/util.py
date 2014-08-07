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
