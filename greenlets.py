#!/usr/bin/python
# Author: Tapas Sharma
# This is a simple code written to show how we can use greelets
# to end all the threads if one thread exits
import sys
import gevent
import time
from gevent import Timeout
from gevent import monkey
monkey.patch_all()
from gevent.queue import Queue

# A new exception that we can raise
# when all the tasks complete
class TaskComplete(Exception):
    pass

# The class that will spwan the threads. Has the following methods
# __init__ - the constructor for this class (rem: c++)
# long_func - a tiny thread that runs in a loop
# short_func - add an element to the queue and exits
# stop - to close all the threads spawned.
class Worker():
    def __init__(self):
        self.threads = []
        self.queue = Queue()

    def long_func(self, th, seed):
        k = 0
        while k < 10000:
            print "LOG: Inside the long function Thread: ", th, " Seed: ", seed
            time.sleep(.1)
        print "LOG: Long function is out of the loop", seed
        self.queue.put_nowait(seed)

    def short_func(self, th, seed):
        print "LOG: Inside the short function Thread:", th, " Seed: ", seed
        self.queue.put_nowait(seed)

    def start(self, seed):
        print "INFO: Initializing the threads..."
        self.threads.append(gevent.spawn(self.long_func, 1, seed))
        gevent.sleep(1)
        self.threads.append(gevent.spawn(self.short_func, 2, seed))
        while self.queue.empty():
            print "INFO: Queue is empty %s" % seed
            gevent.sleep(0)
        raise TaskComplete

    def stop(self):
        gevent.killall(self.threads)

# Our main function that swapns the greenlets
def maingreenlet():
    test_class = Worker()
    i = 0
    try:
        gevent.with_timeout(5, test_class.start, i)
    except Timeout:
        print 'Exception of timeout'
        test_class.stop()
        print 'Exiting all greenlets'
    except TaskComplete:
        print "Task complete message from queue", test_class.queue.get()
    except:
        print "Error: Unknown exception occured", sys.exc_info()


if __name__ == '__main__':
    maingreenlet()
