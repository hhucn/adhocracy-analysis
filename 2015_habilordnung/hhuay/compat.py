try:
    compat_str = unicode
except NameError:  # Python 3
    compat_str = str

try:
    import urllib.request as compat_urllib_request
except NameError:  # Python 2
    import urllib
    compat_urllib_request = urllib

