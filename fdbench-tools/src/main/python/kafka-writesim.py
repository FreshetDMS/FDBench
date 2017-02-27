import numpy as np
import random

BYTES_PER_KB = 1024.0
MICROSECS_PER_SEC = 1000000


class Page(object):
    def __init__(self, f, created_at, dirty=True, page_size_kb=4.0):
        self.f = f
        self.created_at = created_at
        self.last_modified_at = None
        self.dirty = dirty
        self.page_size_kb = page_size_kb
        self.current_size = 0.0

    def is_dirty(self):
        return self.dirty

    def mark_not_dirty(self):
        self.dirty = False

    def is_expired(self, current_time, expiration):
        return current_time - self.created_at > expiration

    def write(self, size_kb, current_time):
        if not self.dirty:
            self.dirty = True

        self.last_modified_at = current_time

        if self.current_size + size_kb > self.page_size_kb:
            extra = size_kb - (self.page_size_kb - self.current_size)
            self.current_size = self.page_size_kb
            return extra
        else:
            self.current_size += size_kb
            return 0

    def size_kb(self):
        return self.page_size_kb


# TODO: Count read misses as well
class PageCache(object):
    def __init__(self, max_size_kbs, page_size_kbs, iops_max_size_kbs):
        self.max_size_kbs = max_size_kbs
        self.page_size_kb = page_size_kbs
        self.pages = []
        self.files = {}
        self.dirty = 0
        self.flushed_pages = 0
        self.iops_max_size_kbs = iops_max_size_kbs
        self.iops = []  # TODO: Add IOP type (read, write)

    def write(self, f, size_kb, current_time):
        if f not in self.files:
            p = Page(f, current_time, page_size_kb=self.page_size_kb)
            self.dirty += 1
            self.pages.append(p)
            self.files[f] = [p]

        remainder = self.files[f][-1].write(size_kb, current_time)
        while remainder != 0:
            p = Page(f, current_time, page_size_kb=self.page_size_kb)
            self.dirty += 1
            self.pages.append(p)
            self.files[f].append(p)
            remainder = self.files[f][-1].write(remainder, current_time)

    def get_expired_pages(self, current_time, expiration):
        expired_pages = []
        for p in self.pages:
            if p.is_expired(current_time, expiration) and p.is_dirty():
                expired_pages.append(p)

        return expired_pages

    def get_dirty_pages(self):
        dirty_pages = []
        for p in self.pages:
            if p.is_dirty():
                dirty_pages.append(p)
        return dirty_pages

    def size(self):
        return len(self.pages) * self.page_size_kb

    def dirty_count(self):
        return self.dirty

    def dirty_size(self):
        return self.dirty * self.page_size_kb

    def flush(self, pages):
        iops = 0
        iops_sizes = []
        pages_to_flush = {}
        self.flushed_pages += len(pages)

        if pages is None or len(pages) == 0:
            return 0, []

        for p in pages:
            p.mark_not_dirty()
            self.dirty -= 1
            if p.f not in pages_to_flush:
                pages_to_flush[p.f] = [p]
            else:
                pages_to_flush[p.f].append(p)

        max_pages_per_write = int(self.iops_max_size_kbs / self.page_size_kb)
        for k, v in pages_to_flush.iteritems():
            iops += (len(v) + max_pages_per_write // 2) // max_pages_per_write
            full_iops = len(v) / max_pages_per_write

            if len(v) % max_pages_per_write != 0:
                iops_sizes.append((len(v) % max_pages_per_write) * self.page_size_kb)

            for i in range(0, full_iops):
                iops_sizes.append(self.iops_max_size_kbs)

        return iops, iops_sizes


class Simulation(object):
    def __init__(self, workload={}, ram_kbs=10000000, mapped_kbs=240000, writeback_interval_secs=5,
                 dirty_expiration_secs=30, dirty_ratio=20, dirty_writeback_ratio=10, page_size_kbs=4,
                 iops_max_size_kbs=256):
        self.workload = workload
        self.ram_kbs = ram_kbs
        self.mapped_kbs = mapped_kbs
        self.max_pcache = ram_kbs - mapped_kbs
        self.free = ram_kbs - mapped_kbs
        self.writeback_interval_secs = writeback_interval_secs
        self.dirty_expiration_secs = dirty_expiration_secs
        self.dirty_ratio = dirty_ratio
        self.dirty_writeback_ratio = dirty_writeback_ratio
        self.page_size_kbs = page_size_kbs
        self.pcache = PageCache(self.max_pcache, self.page_size_kbs, iops_max_size_kbs)
        self.producer_meta = {}
        self.iops = []

    def init(self):
        # TODO: Producer meta should initialize from workload
        self.producer_meta['test-0'] = {'rate': 100000.0, 'msg-size': 134, 'process': 'poison'}

    def gen_writes(self, current_time_microsecs):
        writes = []
        if not self.producer_meta:
            raise ValueError('Producer metadata is empty!')

        for key, value in self.producer_meta.iteritems():
            interval = int(MICROSECS_PER_SEC / value['rate'])
            if 'interval' not in value or 'last-sent' not in value:
                writes.append((key, value['msg-size'] / BYTES_PER_KB))
                value['last-sent'] = current_time_microsecs
                value['interval'] = interval
            elif value['interval'] + value['last-sent'] < current_time_microsecs:
                writes.append((key, value['msg-size'] / BYTES_PER_KB))
                value['last-sent'] = current_time_microsecs
                value['interval'] = interval

        return writes

    def run(self, sim_duration_secs=120):
        sim_duration_microsecs = sim_duration_secs * MICROSECS_PER_SEC
        write_count = 0
        write_kbs = 0.0
        for t in xrange(1, sim_duration_microsecs):
            if t % MICROSECS_PER_SEC == 0:
                print 'Time:', t / MICROSECS_PER_SEC
                print 'Writes:', write_count
                if len(self.iops) > 0:
                    print 'Average IO Size: ', np.mean(self.iops)
                    print 'Total IOP: ', len(self.iops)

            writes = self.gen_writes(t)
            write_count += len(writes)
            for w in writes:
                write_kbs += w[1]
                self.pcache.write(w[0], w[1], t)

            if t % (self.writeback_interval_secs * MICROSECS_PER_SEC) == 0:
                expired = self.pcache.get_expired_pages(t, self.dirty_expiration_secs * MICROSECS_PER_SEC)
                print 'Expired:', len(expired)
                iops_count, iops_sizes = self.pcache.flush(expired)
                self.iops.extend(iops_sizes)

            if self.pcache.dirty_size() > (self.dirty_writeback_ratio * self.ram_kbs) / 100:
                print 'Dirty Writeback Raito Reached.'
                iops_count, iops_sizes = self.pcache.flush(self.pcache.get_dirty_pages())
                self.iops.extend(iops_sizes)

                # TODO: Handle the dirty_ratio case where we block all the pages

        print 'Flushed Pages: ', self.pcache.flushed_pages
        print 'Pages Written: ', len(self.pcache.pages)
        print 'Total Writes in KBs: ', write_kbs
        print 'Average IO Size: ', np.mean(self.iops)
        print 'Average IOPS: ', len(self.iops) / float(sim_duration_secs)


# pdflush daemon flush pages to disk on two occasions. When kernel tells it that the free memory is
# below threshold and when dirty pages are older than dirty_expire_centisecs during regular scheduled
# checkup (every dirty_writeback_centisecs intervals).
class PDFlushDaemon(object):
    def __init__(self, env, interval):
        pass


# Kernel wakes up pdflush daemon when free memory goes below dirty_background_ratio
class Kernel(object):
    pass


# Kafka process (API Thread) handles produce requests and writes them to file system (page cache)
class KafkaAPIThread(object):
    pass


# TODO: Should handle capacity misses, capacity flushes

s = Simulation(ram_kbs=15000000)
s.init()
s.run()
