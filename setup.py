#!/usr/bin/env python
# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import os
from setuptools import setup


VERSION = (0, 0, 2, "")

install_deps = ['psycopg2', 'mummy']
test_deps = install_deps + ['gevent', 'fuzzy', 'nose']

setup(
    name="datahog",
    description="a shardable postgresql-backed data store",
    packages=["datahog", "datahog.api", "datahog.const", "datahog.db"],
    version='.'.join([_f for _f in map(str, VERSION) if _f]),
    install_requires=install_deps,
    tests_require=test_deps,
    test_suite='nose.collector',
)
