from influxdb import InfluxDBClient
import logging

class InfluxDB(object):

    def __init__(self, hostname='localhost', port=8086, username='root', password='root', db_name='crawler'):
        self.__hostname__ = hostname
        self.__port__ = port
        self.__username__ = username
        self.__password__ = password
        self.__db_name__ = db_name

        self.logger = logging.getLogger('main')
        
        # Perform connection
        self.connect_to_db()

    def connect_to_db(self):        
        # Create an InfluxDB client
        self.client = InfluxDBClient(self.__hostname__, self.__port__, self.__username__, self.__password__, self.__db_name__)

        # Create rentention policy
        self.client.create_retention_policy('collect_policy', '7d', 3, default=True)

        # Get version
        version = self.client.ping()

        if not version:
            self.logger.error(f'Failed to receive version from {self.__hostname__}:{self.__port__}')
            return

        self.logger.info(f'Connected to influxdb server {self.__hostname__}:{self.__port__} v{version}')

    def write_data(self, data):
        # print(f'Data: {data}')
        self.client.write_points(data)