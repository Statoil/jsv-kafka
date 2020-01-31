#!/usr/bin/env python
# encoding: utf-8
"""
constant.py
"""


class Constant:
    # Dev constant
    BOOTSTRAP_SERVERS_DEV = 'localhost:9093'
    SCHEMA_REGISTRY_URL_DEV = 'http://localhost:8081'

    # Test constant
    BOOTSTRAP_SERVERS_TEST = 'brokers.cold.jsvprod.net:9092'
    SCHEMA_REGISTRY_URL_TEST = 'http://schemaregistry1.cold.jsvprod.net:8081'
