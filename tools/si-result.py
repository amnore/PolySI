#!/bin/python3
from functools import reduce
from pathlib import Path
from typing import Any, Callable, Dict, Tuple
from json import loads
from itertools import chain
import re

Param = Tuple[int, int, int, int]

dbcop: Dict[str, Dict[Param, Dict[int, Tuple[bool, float]]]] = {}
si: Dict[str, Dict[Param, Dict[int, Tuple[bool, float, float, float]]]] = {}

for dataset in Path('.').glob('*'):
    dbcop[dataset.name] = {}

    for params in dataset.glob('*'):
        if params.name == 'stats.db':
            continue

        param = tuple(map(int, params.name.split('_')))
        dbcop[dataset.name][param] = {}

        for hist in params.glob('*'):
            histid = int(hist.name.split('-')[1])
            obj = loads((hist / 'result_log.json').open().readlines()[-1])
            dbcop[dataset.name][param][histid] = \
                (obj['minViolation'] == 'ok', obj['duration'] * 1000)

"""
DBCopExecutions/antidote_all_writes/3_30_20_180/hist-00000/history.bincode
Sessions count: 4
Transactions count: 91
Events count: 1980
WR edges count: 858
SO edges count: 87
Constraints count: 4548

>>> Overall runtime = 1513ms
  construct: 41ms
  solve: 1472ms
[[[[ REJECT ]]]]

"""
with open('/tmp/log') as log:
    while True:
        lines = []
        while True:
            l = log.readline()
            if not l:
                break
            if not re.match(r'^(?:Conflicts|Edge|Constraint):', l):
                lines.append(l)
                if len(lines) == 13:
                    break
        if len(lines) < 13:
            break

        loc = lines[0]
        [_, dataset, params, hist, __] = loc.split('/')
        get_time = lambda l: float(l.split()[-1][:-2])
        total_time = get_time(lines[8])
        construct_time = get_time(lines[9])
        solve_time = get_time(lines[10])
        result = lines[11].startswith('[[[[ ACCEPT ]]]]')
        
        param = tuple(map(int, params.split('_')))
        histid = int(hist.split('-')[-1])
        if dataset not in si:
            si[dataset] = {}
        if param not in si[dataset]:
            si[dataset][param] = {}
        if histid not in si[dataset][param]:
            si[dataset][param][histid] = {}
        si[dataset][param][histid] = (result, total_time, construct_time, solve_time)

total_n: Dict[Tuple[str, Param], int] = {}
total_si_time: Dict[Tuple[str, Param], float] = {}
total_dbcop_time: Dict[Tuple[str, Param], float] = {}
(true_pos, true_neg, false_pos, false_neg) = (0, 0, 0, 0)
total_rej_n: Dict[Tuple[str, Param], int] = {}
total_rej_time: Dict[Tuple[str, Param], float] = {}
total_construct_time: Dict[Tuple[str, Param], float] = {}
total_solve_time: Dict[Tuple[str, Param], float] = {}

def for_each_result(f: Callable[[Tuple[str, Param, int], 
                                 Tuple[bool, float],
                                 Tuple[bool, float]],
                                Any]):
    global si, dbcop
    datasets = ('galera_all_writes', 'galera_partition_writes',
                'roachdb_all_writes', 'roachdb_partition_writes')
    param = (3, 30, 20, 180)
    for dataset in datasets:
        for histid in si[dataset][param].keys():
            f((dataset, param, histid), dbcop[dataset][param][histid], si[dataset][param][histid])

def sum_time(p, q, r):
    global total_n, total_si_time, total_dbcop_time, total_rej_n, total_rej_time, total_construct_time, total_solve_time
    k = (p[0], p[1])
    def add(d, t):
        d[k] = d.get(k, 0) + t
    add(total_n, 1)
    add(total_dbcop_time, q[1])
    add(total_si_time, r[1])
    add(total_construct_time, r[2])
    add(total_solve_time, r[3])
    if not r[0]:
        add(total_rej_n, 1)
        add(total_rej_time, r[1])

def sum_result(p, q, r):
    global true_pos, true_neg, false_pos, false_neg
    if not r[0]:
        if not q[0]:
            true_pos += 1
        else:
            false_pos += 1
    else:
        if r[0]:
            true_neg += 1
        else:
            false_neg += 1

def do_stats(p, q, r):
    sum_time(p, q, r)
    sum_result(p, q, r)

for_each_result(do_stats)

print('||Number|SI Time|DBCop Time|\n|-|-|-|-|')
for k in total_n.keys():
    n = total_n[k]
    print('|{}|{}|{:.2f}|{:.2f}|'.format(k[0], n, total_si_time[k] / n, total_dbcop_time[k] / n))

print('\n||True|False|\n|-|-|-|')
print('|Positive|{}|{}|'.format(true_pos, false_pos))
print('|Negative|{}|{}|'.format(true_neg, false_neg))

print('\n||Accept N|Reject N|Accept Time|Reject Time|\n|-|-|-|-|-|')
for k in total_si_time.keys():
    rej_n = total_rej_n[k] 
    ac_n = total_n[k] - rej_n
    rej_t = total_rej_time[k]
    ac_t = total_si_time[k] - rej_t
    print('|{}|{}|{}|{:.2f}|{:.2f}|'.format(k[0], ac_n, rej_n, ac_t / ac_n, rej_t / rej_n))

print('\n||Construct Time|Solve Time|\n|-|-|-|')
for k in total_n.keys():
    n = total_n[k]
    print('|{}|{:.2f}|{:.2f}|'.format(k[0], total_construct_time[k] / n, total_solve_time[k] / n))