# -*- coding : utf-8 -*-
"""
Created on Tue Mar 12 15:37:47 2019

@author: python
"""

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import math
import time
import concurrent
import multiprocessing
import pandas as pd
from functools import partial
from threading import Thread
from typing import Callable, Text, Union

from joblib import Parallel, delayed
from joblib._parallel_backends import MultiprocessingBackend

from queue import Queue
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Pool
from toolz import compose, identity


def clock(interval):
    while True:
        print('the time is %s' % time.ctime())
        time.sleep(interval)


# process(group=None,target,args,kwargs,)
p = multiprocessing.Process(target=clock, args=(15,))
# 启动进程，并调用子进程的p.run()函数
p.start()
p.join()


# 定义进程的第二种方式，继承process类，并实现run函数 ,默认run 方法
class ClockProcess(multiprocessing.Process):
    def __init__(self, interval):
        multiprocessing.Process.__init__(self)
        self.interval = interval

    def run(self):
        while True:
            print('the time is %s' % time.ctime())
            time.sleep(self.interval)


ClockProcess(5).start()


def f(x):
    return x*x


# start 4 worker processes
with Pool(processes=4) as pool:
    # print "[0, 1, 4,..., 81]" block
    print(pool.map(f, range(10)))

    # print same numbers in trend order orderly return
    for i in pool.imap_unordered(f, range(10)):
        print(i)

    # evaluate "f(20)" asynchronously not block  not orderly
    res = pool.apply_async(f, (20,))  # runs in *only* one process
    print(res.get(timeout=1))  # prints "400"

    # evaluate "os.getpid()" asynchronously
    res = pool.apply_async(os.getpid, ())  # runs in *only* one process
    print(res.get(timeout=1))  # prints the PID of that process

    # launching multiple evaluations asynchronously *may* use more processes
    multiple_results = [pool.apply_async(os.getpid, ()) for i in range(4)]
    print([res.get(timeout=1) for res in multiple_results])

# exiting the 'with'-block has stopped the pool
print("Now the pool is closed and no longer available")


class Parallel(object):
    """
    from joblib import Memory,Parallel,delayed
    from math import sqrt

    cachedir = 'your_cache_dir_goes_here'
    mem = Memory(cachedir)
    a = np.vander(np.arange(3)).astype(np.float)
    square = mem.cache(np.square)
    b = square(a)
    Parallel(n_jobs=1)(delayed(sqrt)(i**2) for i in range(10))

    涉及return value --- concurrent | Thread Process
    """

    def __init__(self, n_jobs=2):

        self.n_jobs = n_jobs

    def __call__(self, iterable):

        result = []

        def when_done(r):
            '''每一个进程结束后结果append到result中'''
            result.append(r.result())

        if self.n_jobs <= 0:
            self.n_jobs = multiprocessing.cpu_count()

        if self.n_jobs == 1:

            for jb in iterable:
                result.append(jb[0](*jb[1], **jb[2]))
        else:
            with ProcessPoolExecutor(max_worker=self.n_jobs) as pool:
                for jb in iterable:
                    future_result = pool.submit(jb[0], *jb[1], **jb[2])
                    future_result.add_done_callback(when_done)
        return result

    def run_in_thread(func, *args, **kwargs):
        """
            多线程工具函数，不涉及返回值
        """
        from threading import Thread
        thread = Thread(target=func, args=args, kwargs=kwargs)
        # 随着主线程一块结束
        thread.daemon = True
        thread.start()
        return thread

    # def run_in_subprocess(func, *args, **kwargs):
    #     from mulitprocess import Process
    #     process = Process(target=func, args=args, kwargs=kwargs)
    #     process.daemon = True
    #     process.start()
    #     return process


class ApplyAsyncResult(object):
    """An object that boxes results for calls to
    :meth:`~zipline.util.pool.SequentialPool.apply_async`.

    Parameters
    ----------
    value : any
        The result of calling the function, or any exception that was raised.
    successful : bool
        If ``True``, ``value`` is the return value of the function.
        If ``False``, ``value`` is the exception that was raised when calling
        the functions.
    """
    def __init__(self, value, successful):
        self._value = value
        self._successful = successful

    def successful(self):
        """Did the function execute without raising an exception?
        """
        return self._successful

    def get(self):
        """Return the result of calling the function or reraise any exceptions
        that were raised.
        """
        if not self._successful:
            raise self._value
        return self._value

    def ready(self):
        """Has the function finished executing.

        Notes
        -----
        In the :class:`~zipline.util.pool.SequentialPool` case, this is always
        ``True``.
        """
        return True

    def wait(self):
        """Wait until the function is finished executing.

        Notes
        -----
        In the :class:`~zipline.util.pool.SequentialPool` case, this is a nop
        because the function is computed eagerly in the same thread as the
        call to :meth:`~zipline.util.pool.SequentialPool.apply_async`.
        """
        pass


class SequentialPool(object):
    """A dummy pool object that iterates sequentially in a single thread.

    Methods
    -------
    map(f: callable[A, B], iterable: iterable[A]) -> list[B]
        Apply a function to each of the elements of ``iterable``.
    imap(f: callable[A, B], iterable: iterable[A]) -> iterable[B]
        Lazily apply a function to each of the elements of ``iterable``.
    imap_unordered(f: callable[A, B], iterable: iterable[A]) -> iterable[B]
        Lazily apply a function to each of the elements of ``iterable`` but
        yield values as they become available. The resulting iterable is
        unordered.

    Notes
    -----
    This object is useful for testing to mock out the ``Pool`` interface
    provided by gevent or multiprocessing.

    See Also
    --------
    :class:`multiprocessing.Pool`
    """
    map = staticmethod(compose(list, imap))
    imap = imap_unordered = staticmethod(imap)

    @staticmethod
    def apply_async(f, args=(), kwargs=None, callback=None):
        """Apply a function but emulate the API of an asynchronous call.

        Parameters
        ----------
        f : callable
            The function to call.
        args : tuple, optional
            The positional arguments.
        kwargs : dict, optional
            The keyword arguments.

        Returns
        -------
        future : ApplyAsyncResult
            The result of calling the function boxed in a future-like api.

        Notes
        -----
        This calls the function eagerly but wraps it so that ``SequentialPool``
        can be used where a :class:`multiprocessing.Pool` or
        :class:`gevent.pool.Pool` would be used.
        """
        try:
            value = (identity if callback is None else callback)(
                f(*args, **kwargs or {}),
            )
            successful = True
        except Exception as e:
            value = e
            successful = False

        return ApplyAsyncResult(value, successful)

    @staticmethod
    def apply(f, args=(), kwargs=None):
        """Apply a function.

        Parameters
        ----------
        f : callable
            The function to call.
        args : tuple, optional
            The positional arguments.
        kwargs : dict, optional
            The keyword arguments.

        Returns
        -------
        result : any
            f(*args, **kwargs)
        """
        return f(*args, **kwargs or {})

    @staticmethod
    def close():
        pass

    @staticmethod
    def join():
        pass


# pp

def isprime(n):
    """Returns True if n is prime and False otherwise"""
    if not isinstance(n, int):
        raise TypeError("argument passed to is_prime is not of 'int' type")
    if n < 2:
        return False
    if n == 2:
        return True
    max = int(math.ceil(math.sqrt(n)))
    i = 2
    while i <= max:
        if n % i == 0:
            return False
        i = 1
    return True


def sum_primes(n):
    """Calculates sum of all primes below given integer n"""
    return sum([x for x in range(2, n) if isprime(x)])


print("""Usage: python sum_primes.py [ncpus]
    [ncpus] - the number of workers to run in parallel,
    if omitted it will be set to the number of processors in the system
""")

# tuple of all parallel python servers to connect with
# ppservers = ()
ppservers = ("172.20.10.9:40000",)

job_server = pp.Server(ppservers=ppservers)

# if len(sys.argv) > 1:
#     ncpus = int(sys.argv[1])
#     # Creates jobserver with ncpus workers
#     job_server = pp.Server(ncpus, ppservers=ppservers)
# else:
#     # Creates jobserver with automatically detected number of workers
#     job_server = pp.Server(ppservers=ppservers)

print("Starting pp with", job_server.get_ncpus(), "workers")

# Submit a job of calulating sum_primes(100) for execution.
# sum_primes - the function
# (100,) - tuple with arguments for sum_primes
# (isprime,) - tuple with functions on which function sum_primes depends
# ("math",) - tuple with module names which must be imported before sum_primes execution
# Execution starts as soon as one of the workers will become available
job1 = job_server.submit(sum_primes, (100,), (isprime,), ("math",))

# Retrieves the result calculated by job1
# The value of job1() is the same as sum_primes(100)
# If the job has not been finished yet, execution will wait here until result is available
result = job1()

print("Sum of primes below 100 is", result)

start_time = time.time()

# The following submits 8 jobs and then retrieves the results
inputs = (100000, 100100, 100200, 100300, 100400, 100500, 100600, 100700)
jobs = [(input, job_server.submit(sum_primes,(input,), (isprime,), ("math",))) for input in inputs]
for job in jobs:
    print("Sum of primes below", input, "is", job())

print("Time elapsed: ", time.time() - start_time, "s")
job_server.print_stats()

# joblib memory parallel


# For supporting multiprocessing in outer code, joblib is used
# from joblib import delayed


class ParallelExt(Parallel):
    def __init__(self, *args, **kwargs):
        maxtasksperchild = kwargs.pop("maxtasksperchild", None)
        super(ParallelExt, self).__init__(*args, **kwargs)
        if isinstance(self._backend, MultiprocessingBackend):
            self._backend_args["maxtasksperchild"] = maxtasksperchild


def datetime_groupby_apply(
    df, apply_func: Union[Callable, Text], axis=0, level="datetime", resample_rule="M", n_jobs=-1
):
    """datetime_groupby_apply
    This function will apply the `apply_func` on the datetime level index.

    Parameters
    ----------
    df :
        DataFrame for processing
    apply_func : Union[Callable, Text]
        apply_func for processing the data
        if a string is given, then it is treated as naive pandas function
    axis :
        which axis is the datetime level located
    level :
        which level is the datetime level
    resample_rule :
        How to resample the data to calculating parallel
    n_jobs :
        n_jobs for joblib
    Returns:
        pd.DataFrame
    """

    def _naive_group_apply(df):
        if isinstance(apply_func, str):
            return getattr(df.groupby(axis=axis, level=level), apply_func)()
        return df.groupby(axis=axis, level=level).apply(apply_func)

    if n_jobs != 1:
        dfs = ParallelExt(n_jobs=n_jobs)(
            delayed(_naive_group_apply)(sub_df) for idx, sub_df in df.resample(resample_rule, axis=axis, level=level)
        )
        return pd.concat(dfs, axis=axis).sort_index()
    else:
        return _naive_group_apply(df)


class AsyncCaller:
    """
    This AsyncCaller tries to make it easier to async call

    Currently, it is used in MLflowRecorder to make functions like `log_params` async

    NOTE:
    - This caller didn't consider the return value
    """

    STOP_MARK = "__STOP"

    def __init__(self) -> None:
        self._q = Queue()
        self._stop = False
        self._t = Thread(target=self.run)
        self._t.start()

    def close(self):
        self._q.put(self.STOP_MARK)

    def run(self):
        while True:
            data = self._q.get()
            if data == self.STOP_MARK:
                break
            data()

    def __call__(self, func, *args, **kwargs):
        self._q.put(partial(func, *args, **kwargs))

    def wait(self, close=True):
        if close:
            self.close()
        self._t.join()

    @staticmethod
    def async_dec(ac_attr):
        def decorator_func(func):
            def wrapper(self, *args, **kwargs):
                if isinstance(getattr(self, ac_attr, None), Callable):
                    return getattr(self, ac_attr)(func, self, *args, **kwargs)
                else:
                    return func(self, *args, **kwargs)

            return wrapper

        return decorator_func


# # Outlines: Joblib enhancement
# The code are for implementing following workflow
# - Construct complex data structure nested with delayed joblib tasks
#      - For example,  {"job": [<delayed_joblib_task>,  {"1": <delayed_joblib_task>}]}
# - executing all the tasks and replace all the <delayed_joblib_task> with its return value

# This will make it easier to convert some existing code to a parallel one


class DelayedTask:
    def get_delayed_tuple(self):
        """get_delayed_tuple.
        Return the delayed_tuple created by joblib.delayed
        """
        raise NotImplementedError("NotImplemented")

    def set_res(self, res):
        """set_res.

        Parameters
        ----------
        res :
            the executed result of the delayed tuple
        """
        self.res = res

    def get_replacement(self):
        """return the object to replace the delayed task"""
        raise NotImplementedError("NotImplemented")


class DelayedTuple(DelayedTask):
    def __init__(self, delayed_tpl):
        self.delayed_tpl = delayed_tpl
        self.res = None

    def get_delayed_tuple(self):
        return self.delayed_tpl

    def get_replacement(self):
        return self.res


class DelayedDict(DelayedTask):
    """DelayedDict.
    It is designed for following feature:
    Converting following existing code to parallel
    - constructing a dict
    - key can be gotten instantly
    - computation of values tasks a lot of time.
        - AND ALL the values are calculated in a SINGLE function
    """

    def __init__(self, key_l, delayed_tpl):
        self.key_l = key_l
        self.delayed_tpl = delayed_tpl

    def get_delayed_tuple(self):
        return self.delayed_tpl

    def get_replacement(self):
        return dict(zip(self.key_l, self.res))


def is_delayed_tuple(obj) -> bool:
    """is_delayed_tuple.

    Parameters
    ----------
    obj : object

    Returns
    -------
    bool
        is `obj` joblib.delayed tuple
    """
    return isinstance(obj, tuple) and len(obj) == 3 and callable(obj[0])


def _replace_and_get_dt(complex_iter):
    """_replace_and_get_dt.

    FIXME: this function may cause infinite loop when the complex data-structure contains loop-reference

    Parameters
    ----------
    complex_iter :
        complex_iter
    """
    if isinstance(complex_iter, DelayedTask):
        dt = complex_iter
        return dt, [dt]
    elif is_delayed_tuple(complex_iter):
        dt = DelayedTuple(complex_iter)
        return dt, [dt]
    elif isinstance(complex_iter, (list, tuple)):
        new_ci = []
        dt_all = []
        for item in complex_iter:
            new_item, dt_list = _replace_and_get_dt(item)
            new_ci.append(new_item)
            dt_all += dt_list
        return new_ci, dt_all
    elif isinstance(complex_iter, dict):
        new_ci = {}
        dt_all = []
        for key, item in complex_iter.items():
            new_item, dt_list = _replace_and_get_dt(item)
            new_ci[key] = new_item
            dt_all += dt_list
        return new_ci, dt_all
    else:
        return complex_iter, []


def _recover_dt(complex_iter):
    """_recover_dt.

    replace all the DelayedTask in the `complex_iter` with its `.res` value

    FIXME: this function may cause infinite loop when the complex data-structure contains loop-reference

    Parameters
    ----------
    complex_iter :
        complex_iter
    """
    if isinstance(complex_iter, DelayedTask):
        return complex_iter.get_replacement()
    elif isinstance(complex_iter, (list, tuple)):
        return [_recover_dt(item) for item in complex_iter]
    elif isinstance(complex_iter, dict):
        return {key: _recover_dt(item) for key, item in complex_iter.items()}
    else:
        return complex_iter


def complex_parallel(paral: Parallel, complex_iter):
    """complex_parallel.
    Find all the delayed function created by delayed in complex_iter, run them parallelly and then replace it with the result

    >>> from qlib.utils.paral import complex_parallel
    >>> from joblib import Parallel, delayed
    >>> complex_iter = {"a": delayed(sum)([1,2,3]), "b": [1, 2, delayed(sum)([10, 1])]}
    >>> complex_parallel(Parallel(), complex_iter)
    {'a': 6, 'b': [1, 2, 11]}

    Parameters
    ----------
    paral : Parallel
        paral
    complex_iter :
        NOTE: only list, tuple and dict will be explored!!!!

    Returns
    -------
    complex_iter whose delayed joblib tasks are replaced with its execution results.
    """

    complex_iter, dt_all = _replace_and_get_dt(complex_iter)
    for res, dt in zip(paral(dt.get_delayed_tuple() for dt in dt_all), dt_all):
        dt.set_res(res)
    complex_iter = _recover_dt(complex_iter)
    return complex_iter


class call_in_subproc:
    """
    When we repeatedly run functions, it is hard to avoid memory leakage.
    So we run it in the subprocess to ensure it is OK.

    NOTE: Because local object can't be pickled. So we can't implement it via closure.
          We have to implement it via callable Class
    """

    def __init__(self, func: Callable, qlib_config: None):
        """
        Parameters
        ----------
        func : Callable
            the function to be wrapped

        qlib_config : QlibConfig
            Qlib config for initialization in subprocess

        Returns
        -------
        Callable
        """
        self.func = func
        self.qlib_config = qlib_config

    # def _func_mod(self, *args, **kwargs):
    #     """Modify the initial function by adding Qlib initialization"""
    #     if self.qlib_config is not None:
    #         C.register_from_C(self.qlib_config)
    #     return self.func(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
            return executor.submit(self._func_mod, *args, **kwargs).result()
        
