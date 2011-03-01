"""
Like ThreadPool, only less flexible and harder to understand. Plus
possibly buggy. But unlike ThreadPool, it worked when I used it.
"""

import logging
import sys
import threading
import time

logger = logging.getLogger(__name__)

class ValueThread(threading.Thread):
    """
    A thread which retains the return value of the object it calls.
    """
    def run(self):
        if self._Thread__target:
            try:
                self.value = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
            except Exception, e:
                self.exception = e

def launch(func, nthreads=5, argtuples=None, show_progress=False, timeout=None, argiter=None, report_every=0):
    """Multithread repeated calls to a function. Designed to help with non-threadsafe classes like sands.
       For those classes, each thread gets its own copy of the object, by running objfactory
       n times.

       func = the function to run on each item
       argtuples = list of lists/tuples; each one will be the argument list for one call to func
       nthreads = maximum number of threads
       show_progress = what you expect

       Returns a list of results. Each line of results looks like this:
          ( argsused, returnvalue )
       If you were calling a function with a single argument, your input 
       is line[0][0] and your return value is line[1].

    """
    if argtuples:
        try:
            ## Copy argument list so we can pop it to death.
            argtuples = argtuples[:]
            logger.debug('Starting %s of %s with %s threads.', len(argtuples), func, nthreads)
        except TypeError:
            argiter = argtuples
            argtuples = None
    if argiter:
        logger.debug('Starting iterator of %s with %s threads.', func, nthreads)
    ## The first n integers are our markers for which thread is free.
    ready_threads = range(nthreads)
    results = ThreadrunResultSet()
    running_threads = {}
    if show_progress:
        ntasks = len(argtuples) if argtuples else 10000
        if not report_every:
            ## Arbitrary choice-- report every 1% (capped at 100, floor at 5)
            report_every = min(100, ntasks/100)
            report_every = max(5, report_every)
        tic = time.time()
    basewait = wait = 0.000001
    running = True
    ndone = 0
    msg = ''
    while running:
        if ready_threads:
            this = ready_threads.pop()
            try:
                args = argtuples.pop() if argtuples else argiter.next()
                if not isinstance(args, (list, tuple)):
                    args = [args]
                t = ValueThread(target=func, args=args)
                t.start()
                running_threads[t] = this
            except (StopIteration, AttributeError):
                running = False
        if show_progress :
            toc = time.time()
            elapsed = toc - tic
            if argtuples and (len(argtuples) % report_every) == 0:
                nleft = len(argtuples)
                ndone = ntasks - nleft
                persecond = ndone/float(elapsed)
                timeleft = nleft / persecond
                eta = toc + timeleft
                newmsg = "%s tasks left - %s done in %s seconds." % (nleft, ntasks-nleft, int(elapsed))
                estimate = " (ETA: %s)" % time.ctime(eta)
            elif ndone % report_every == 0:
                persecond = ndone/float(elapsed)
                newmsg = "%s done in %s seconds." % (ndone, int(elapsed))
                estimate = ''
            if newmsg != msg:
                sys.stderr.write("%s%s\n" % (newmsg, estimate))
                msg = newmsg
            sys.stderr.flush()
        anydone = False
        for t in running_threads.keys():
            if not t.isAlive():
                anydone = True
                ndone += 1
                results.add_result(t)
                if running:
                    ready_threads.append(running_threads[t])
                del running_threads[t]
        time.sleep(0.00001)
        #if anydone:
        #    logger.debug('Wait reached %s.', wait)
        #    wait = basewait
        #else:
        #    wait = wait * 1.2
        #    time.sleep(wait)

    for t in running_threads.keys():
        # since we can't put a timeout in the things being threaded, timeout here
        t.join(timeout)
        if t.isAlive():
            logger.debug('Thread.join() call timed out')
        results.add_result(t)
        del running_threads[t]
    return results

class ThreadrunResultSet(list, object):
    def __init__(self):
        self.exceptions = []

    def add_result(self, t):
        """Takes a ValueThread and appends its data to self or
           self.exceptions, as appropriate."""
        if hasattr(t, 'value'):
            self.append((t._Thread__args, t.value))
            del t.value
        elif hasattr(t, 'exception'):
            self.exceptions.append((t._Thread__args, t.exception))
            del t.exception
