"""Manual, bounded source-access check. Does not publish or alter timetable data."""
import json
import socket
import time
import requests
from .shared_collector import Source
from .pipeline import normalize, utcnow


def main():
    report={}
    try:
        response=requests.head('https://github.com',timeout=(8,8),allow_redirects=False)
        report['control_http']=response.status_code
    except requests.RequestException as error:
        report['control_error']=str(error)
    host='enquiry.indianrail.gov.in'
    try:
        addresses=socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)
        report['dns']=[{'family':family,'address':address[0]} for family,kind,proto,canon,address in addresses]
        family,kind,proto,canon,address=addresses[0]
        started=time.monotonic()
        with socket.socket(family,kind,proto) as connection:
            connection.settimeout(10)
            connection.connect(address)
        report['tcp_443']='connected'
        report['connect_seconds']=round(time.monotonic()-started,2)
    except OSError as error:
        report['tcp_443']='failed'
        report['connection_error']=str(error)
        print(json.dumps(report));return
    try:
        response=requests.head('https://'+host+'/',timeout=(10,15),allow_redirects=False)
        report['railway_http']=response.status_code
        # Respect an explicit refusal; do not make an API call after one.
        if response.status_code in (401,403,429):
            report['schedule']='not requested after refusal'
        else:
            source=Source()
            try:
                # A single normal read, without the collector's transport retries.
                payload=source._request_once('service=TrainRunningMob&subService=GetTrainSchedule&trainNo=12617&startDate=')
                train=normalize({'source':'NTES','fetched_at':utcnow(),'payload':payload})
                report['schedule']={'number':train['number'],'stops':len(train['stops']),'valid_to':train['valid_to']}
            finally:source.close()
    except Exception as error:
        report['source_error']=type(error).__name__+': '+str(error)
    print(json.dumps(report))

if __name__=='__main__':main()
