import threading


class ReadWriteLock:
    """ A lock object that allows for simultaneous reads but only 1 write """

    def __init__(self):
        self.read_cond = threading.Condition(threading.Lock())
        self.readers = 0

    def acquire_write(self):
        """ Acquire write lock. Blocks until there are no acquired read or write locks. """
        self.read_cond.acquire()
        while self.readers > 0:
            # Wait for current reads to finish before writing
            self.read_cond.wait()

    def release_write(self):
        """ Release write lock. """
        self.read_cond.release()

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if another thread has acquired write lock. """
        # Acquire the read cond lock, so we can add to the number of readers
        # If a write operation is occurring this lock will be acquired until the write is done
        self.read_cond.acquire()
        try:
            self.readers += 1
        finally:
            # Release read lock so other reads can occur
            self.read_cond.release()

    def release_read(self):
        """ Release a read lock. """
        # Acquire the read cond lock, so we can subtract the number of readers
        self.read_cond.acquire()
        try:
            self.readers -= 1
            if not self.readers:
                # Notify all threads there are no readers (can write)
                self.read_cond.notifyAll()
        finally:
            self.read_cond.release()
