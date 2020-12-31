#!/usr/bin/env python
# -*- coding: utf-8 -*-

# based on: https://github.com/kofemann/pgtune ( original code published under public domain license. )

#
# Uasge:
#    ./pgtune.py -c 20 ->> /var/lib/pgsql/10/data/postgresql.conf
#

from __future__ import print_function
import string
import getopt
import sys
from math import floor, log
from distutils.version import LooseVersion

B = 1
K = 1024
M = K * K
G = K * M

DATA_SIZES = {'b': B, 'k': K, 'm': M, 'g': G}
SIZE_SUFFIX = ["", "KB", "MB", "GB", "TB"]

DB_MEMORY_PCT= 0.75

def get_size(s):
    last_symbol = s[-1:].lower()
    if last_symbol in string.digits:
        return long(s)

    if not DATA_SIZES.has_key(last_symbol):
        raise Exception('Invalid format: %s' % s)

    return long(s[:-1]) * DATA_SIZES[last_symbol]


def available_memory():
    meminfo = {}
    with open('/proc/meminfo') as f:
        for line in f:
            s = line.split(': ')
            meminfo[s[0]] = s[1].split()[0].strip()


    return int(   meminfo['MemTotal']  ) * 1024


def beautify(n):
    if type(n) is int and n > 1024:
        return to_size_string(n)
    return str(n)


def to_size_string(n):
    f = int(floor(log(n, 1024)))
    return "%d%s" % (int(n/1024**f), SIZE_SUFFIX[f])


def to_bytes(n, max_size=None):
    v = int(floor(n))
    if max_size is not None:
        return min(max_size, v)
    return v

def calculate(total_mem, max_connections, pg_version):
    pg_conf = {}
    pg_conf['max_connections'] = max_connections
    pg_conf['shared_buffers'] = to_bytes(total_mem/4)
    pg_conf['effective_cache_size'] = to_bytes(total_mem * 3/4)
    pg_conf['work_mem'] = to_bytes((total_mem - pg_conf['shared_buffers']) / (max_connections * 3))
    pg_conf['maintenance_work_mem'] = to_bytes(total_mem/16, 2*G)  # max 2GB
    if LooseVersion(pg_version) < LooseVersion('9.5'):
        pg_conf['checkpoint_segments'] = 64
    else:
      # http://www.postgresql.org/docs/current/static/release-9-5.html
      # max_wal_size = (3 * checkpoint_segments) * 16MB
      pg_conf['min_wal_size'] = '256MB'           
      pg_conf['max_wal_size'] = '4GB'
      # http://www.cybertec.at/2016/06/postgresql-underused-features-wal-compression/
      pg_conf['wal_compression'] = 'on'
    pg_conf['checkpoint_completion_target'] = 0.8
    pg_conf['checkpoint_timeout'] = '15min'
    pg_conf['autovacuum_max_workers'] = 2
    pg_conf['wal_buffers'] = to_bytes(pg_conf['shared_buffers']*0.03, 16*M)  # 3% of shared_buffers, max of 16MB.
    pg_conf['default_statistics_target'] = 300
    pg_conf['synchronous_commit'] = 'off'
    pg_conf['vacuum_cost_delay'] = 50
    pg_conf['vacuum_cost_limit'] = 200
    pg_conf['wal_writer_delay'] = '10s'

    # https://www.postgresql.org/docs/11/static/populate.html
    # https://www.postgresql.org/docs/11/static/non-durability.html
    pg_conf['wal_level']='minimal'
    pg_conf['archive_mode']='off'
    pg_conf['max_wal_senders']= 0
    pg_conf['full_page_writes']='off'
    pg_conf['fsync']='off'
    pg_conf['bgwriter_lru_maxpages']= 0

   # pg_conf['max_stack_depth']='7680kB'
   # pg_conf['data_checksums']='on'
   # pg_conf['client_encoding']='UTF8'
   # pg_conf['server_encoding']='UTF8'
   
    pg_conf['timezone']='UTC'
    pg_conf['datestyle']= "'iso, ymd'"


    pg_conf['auth_delay.milliseconds'] = '5000'
    if LooseVersion(pg_version) >= LooseVersion('10'):
        pg_conf['password_encryption']  = 'scram-sha-256'

    return pg_conf


def usage_and_exit():
    print("Usage: %s [-m <size>] [-c <conn>] [-s] [-S] [-l <listen_addresses>] [-v <version>] [-h]")
    print("")
    print("where:")
    print("  -m <size> : max memory to use, default total available memory")
    print("  -c <conn> : max inumber of concurent client connections, default 100")
    print("  -s        : database located on SSD disks (or fully fit's into memory)")
    print("  -S        : enable tracking of SQL statement execution (require pg >= 9.0)")
    print("  -l <addr> : address(es) on which the server is to listen for incomming connections, default localhost")
    print("  -v <vers> : PostgreSQL version number. Default: 10")
    print("  -h        : print this help message")
    sys.exit(1)


def main():
    mem = None
    max_connections = 40
    have_ssd = True
    enable_stat = False
    listen_addresses = '*'
    pg_version = '12'

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'l:m:c:sSv:h')

        for o, a in opts:
            if o == '-m':
                mem = get_size(a)
            elif o == '-c':
                max_connections = int(a)
            elif o == '-s':
                have_ssd = True
            elif o == '-S':
                enable_stat = True
            elif o == '-l':
                listen_addresses = a
            elif o == '-v':
                pg_version = a
            elif o == '-h':
                usage_and_exit()
            else:
                print('invalid option: %s' % o)
                usage_and_exit()
    except getopt.GetoptError as err:
        print(err)
        usage_and_exit()

    if mem is None:
        mem = available_memory() * DB_MEMORY_PCT

    print("#")
    print("# DW friendly configuration for PostgreSQL %s" % pg_version)
    print("#")
    print("# Config for %s memory and %d connections" % (to_size_string(mem), max_connections))
    print("#")
    pg_conf = calculate(mem, max_connections, pg_version)
    for s in sorted(pg_conf.keys()):
        print("%s = %s" % (s, beautify(pg_conf[s])))
    if have_ssd:
        print("random_page_cost = 1.5")


    print("listen_addresses = '%s'" % (listen_addresses))

    if enable_stat:
        print("shared_preload_libraries = 'auth_delay,pg_stat_statements,auto_explain'")
        print("pg_stat_statements.track = all")
        print("auto_explain.log_min_duration = '5s'")
        print("auto_explain.log_verbose = 'true'")
        print("auto_explain.log_analyze = 'true'")
        print("auto_explain.log_nested_statements = 'true'")

        print("# pg_stats")
        print("track_activities = 'true'")
        print("track_counts = 'true'")
        print("track_io_timing = 'true'")

        print("log_line_prefix = '%m <%d %u %a %r> %%'")
        print("log_temp_files = 0")
        print("log_min_duration_statement = 20")
        print("log_checkpoints = on")
        print("log_lock_waits = on")
    else:
        print("shared_preload_libraries = 'auth_delay'")


    print("#")
    print("# More customisations ")
    print("#")
    print("autovacuum=on")

if __name__ == '__main__':
    main()

