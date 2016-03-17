# coding: utf-8
from Fortum import *
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime,timedelta
import os, time
import logging
logging.basicConfig()

config = {
    'hour': int(os.environ.get('POLL_HOUR',9)),
    'minute': int(os.environ.get('POLL_MINUTE',0)),
    'database': os.environ.get('DATABASE_NAME','SURE'),
    'fortum_user': os.environ.get('FORTUM_USER','user'),
    'fortum_password': os.environ.get('FORTUM_PASSWORD','password'),
    'influx_host': os.environ.get('INFLUX_HOST','192.168.99.100'),
    'influx_port': int(os.environ.get('INFLUX_PORT',8086)),
    'influx_user': os.environ.get('INFLUX_PORT','root'),
    'influx_password': os.environ.get('INFLUX_PASSWORD','root'),
    'data_series': os.environ.get('DATA_SERIES','11996,15953,11995'),
    'verbose': (os.environ.get('VERBOSE','True') == 'True')
}

# Set the timezone, usees environment variable 'TZ', like this: os.environ['TZ']
time.tzset()

sched = BlockingScheduler()
print('type=info msg="polling scheduled" hour=%s minute=%s verbose=%s' % (str(config['hour']), config['minute'], config['verbose'] ))
print('type=info msg="influx settings" host=%s database=%s' % (str(config['influx_host']), config['database'] ))

series = config['data_series'].split(',')

def import_series(seriae):
    print('type=info msg="importing series" id=%s' % seriae)
    fortum = Fortum(seriae)
    fortum.verbose = config['verbose']
    fortum.user = config['fortum_user']
    fortum.password = config['fortum_password']
    fortum.SetInfluxConnection(dbname=config['database'],dbhost=config['influx_host'],dbuser=config['influx_user'],dbpassword=config['influx_password'])
    fortum.CheckDatabase()
    fortum.ImportMissing()
    print('type=info msg="import finished" id=%s' % seriae)

# Run once on startup
print('type=info msg="running once on startup"')
for seriae in series:
    import_series(seriae)
print('type=info msg="startup job finished"')

# Schedule the job!
@sched.scheduled_job('cron', hour=config['hour'], minute=config['minute'])
def import_all_series():
    print('type=info msg="running scheduled job"')
    for seriae in series:
        import_series(seriae)
    print('type=info msg="finished scheduled job"')


# Start the schedule
sched.start()
