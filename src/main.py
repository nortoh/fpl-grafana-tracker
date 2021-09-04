import json
import requests
import datetime
from os import path

from utils.log import Log
from utils.config import Config
from utils.repeating_timer import RepeatingTimer
from net.influx_db import InfluxDB

class Main(object):

    def __init__(self):
        # Start new log file            
        self.logger = Log('main').setup_custom_logger()
        self.version = 1.3

    def start(self):
        self.logger.info(f'Booting FPL scrubber v{self.version}')

        # API urls
        self.outage_api_url = 'https://www.fplmaps.com/customer/outage/CountyOutages.json'
        self.storm_feed_restore_api = 'https://www.fplmaps.com/customer/outage/StormFeedRestoration.json'
        self.green_tickets_api = 'https://www.fplmaps.com/customer/outage/GreenTickets.json'

        # Load config
        self.load_configuration()

        # InfluxDB
        self.influx_db = InfluxDB(self.config.influx_host, self.config.influx_port, self.config.influx_username, self.config.influx_password, self.config.influx_database)

        self.create_timers()

        self.pull_data()
        self.pull_storm_feed_data()
        self.pull_green_tickets_data()

    # Load configuration 'config.json'
    def load_configuration(self):
        self.logger.info('Loading configuration')

        # Configuration name
        config_name = 'config.json'
        config_path = path.join('data', config_name)

        # Configuration template
        config_json = """
        {
            "influx_host":"",
            "influx_port": 8086,
            "influx_username":"",
            "influx_password":"",
            "influx_database":""
        }
        """
        
        # If config does not exist, make a blank one. Otherwise, open current
        if not path.exists(config_path):
            config_file = open(config_path, 'w')
            config_json_obj = json.loads(config_json)
            
            # prettify!
            formatted_json = json.dumps(config_json_obj, indent=2)
            
            config_file.write(formatted_json)
            self.logger.warning('Created config.json but it is empty! Please configure')
            return
        else:
            config_file = open(config_path, 'r')
            config_json_obj = json.load(config_file)
                
        self.config = Config(**config_json_obj)
        self.logger.info('Loaded configuration')


    def create_timers(self):
        #10 min
        ten_min = 900

        self.poll_timer = RepeatingTimer(ten_min, self.poll_tick)
        self.poll_timer.start()

    def poll_tick(self):
        self.pull_data()
        self.pull_storm_feed_data()
        self.pull_green_tickets_data()

    def pull_green_tickets_data(self):
        self.logger.info('Pulling green tickets data')

        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        response = requests.post(self.green_tickets_api)
        data = []

        index = 0

        if response.status_code == 200:
            response_json = json.loads(response.text)

            for outage in response_json['outages']:
                lat = float(outage['lat'])
                lng = float(outage['lng'])
                customers_affected = int(outage['customersAffected'])
                self.logger.info(f'{lat} - {lng} - {customers_affected}')

                green_tickets_body = {
                    'measurement': 'green_tickets',
                    'tags': {
                        'index': index
                    },
                    'time': timestamp,
                    'fields': {
                        'lat': lat,
                        'lng': lng,
                        'num_of_outages': customers_affected
                    }
                }

                data.append(green_tickets_body)
                index += 1

            self.influx_db.write_data(data)
            self.logger.info('Posted to InfluxDB')
            del data
            del index

    def pull_storm_feed_data(self):
        self.logger.info('Pulling storm feed data')

        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        response = requests.post(self.storm_feed_restore_api)
        data = []

        index = 0

        if response.status_code == 200:
            response_json = json.loads(response.text)

            for outage in response_json['outages']:
                lat = float('{:.6f}'.format(outage['lat']))
                lng = float('{:.6f}'.format(outage['lng']))
                customersAffected = int(outage['customersAffected'])
                self.logger.info(f'{lat} - {lng} - {customersAffected}')

                storm_feed_restore_body = {
                    'measurement': 'storm_feed_restore',
                    'tags': {
                        'index': index
                    },
                    'time': timestamp,
                    'fields': {
                        'lat': lat,
                        'lng': lng,
                        'num_of_outages': customersAffected
                    }
                }

                data.append(storm_feed_restore_body)
                index += 1

            self.influx_db.write_data(data)
            self.logger.info('Posted to InfluxDB')
            del data
            del index

    def pull_data(self):
        self.logger.info('Pulling data')

        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        response = requests.post(self.outage_api_url)
        data = []
        
        total_outages = 0
        total_with_service = 0

        if response.status_code == 200:
            response_json = json.loads(response.text)

            total_avg_with_service = 0
            total_counties = len(response_json['outages'])

            for outage in response_json['outages']:
                county_name = outage['County Name']
                customers_out = float(outage['Customers Out'].replace(',',''))
                customers_served = float(outage['Customers Served'].replace(',',''))
                
                total_outages += customers_out
                total_with_service += customers_served - customers_out

                total_with_service_percent = ((customers_served - customers_out) / customers_served) * 100
                self.logger.info(f'{county_name} - {customers_out} - {customers_served} - {total_with_service}')

                fpl_county_outage_body = {
                    'measurement': 'fpl_county_outage',
                    'tags': {
                        'county_name': county_name
                    },
                    'time': timestamp,
                    'fields': {
                        'customers_out': customers_out,
                        'customers_servied': customers_served,
                        'total_with_service': total_with_service_percent
                    }
                }

                data.append(fpl_county_outage_body)

            total_service_body = {
                'measurement': 'total_service',
                'time': timestamp,
                'fields': {
                    'total_with_service': total_with_service,
                    'total_outages': total_outages
                }
            }

            data.append(total_service_body)

            self.influx_db.write_data(data)
            self.logger.info('Posted to InfluxDB')
            del data

if __name__ == '__main__':
    main = Main()
    main.start()