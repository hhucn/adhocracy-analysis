import collections

import progress.bar


class keydefaultdict(collections.defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


class NoProgress(object):
    def __init__(self, stream):
        pass

    def update(self):
        pass


class FileProgress(object):
    def __init__(self, stream):
        pos = stream.tell()
        stream.seek(0, 2)
        self.size = stream.tell()
        stream.seek(pos, 0)
        self.bar = progress.bar.Bar('', max=self.size, suffix='%(percent)d%% ETA %(eta)ds')
        self.stream = stream

    def update(self):
        pos = self.stream.tell()
        self.bar.goto(pos)
