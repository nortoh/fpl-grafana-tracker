from collections import namedtuple
import json

class Config(object):

    def __init__(self, influx_host, influx_port, influx_username, influx_password, influx_database):
        self.influx_host = influx_host
        self.influx_port = influx_port
        self.influx_username = influx_username
        self.influx_password = influx_password
        self.influx_database = influx_database