"""Manual, bounded source-access check. Does not publish or alter timetable data."""
import json
import os
import socket
import ssl
import time
import requests
from requests.adapters import HTTPAdapter
from .shared_collector import Source
from .pipeline import normalize, utcnow

class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self,*args,**kwargs):
        context=ssl.create_default_context()
        context.minimum_version=context.maximum_version=ssl.TLSVersion.TLSv1_2
        kwargs['ssl_context']=context
        return super().init_poolmanager(*args,**kwargs)


def main():
    report={}
    try:
        response=requests.head('https://github.com',timeout=(8,8),allow_redirects=False)
        report['control_http']=response.status_code
    except requests.RequestException as error:report['control_error']=str(error)
    host='enquiry.indianrail.gov.in'
    try:
        addresses=socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)
        report['dns']=[{'family':family,'address':address[0]} for family,kind,proto,canon,address in addresses]
        started=time.monotonic()
        with socket.create_connection((host,443),timeout=10):pass
        report['tcp_443']='connected'
        report['connect_seconds']=round(time.monotonic()-started,2)
    except OSError as error:
        report.update(tcp_443='failed',connection_error=str(error));print(json.dumps(report));return
    report['proxy_env_present']=[name for name in ('HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy') if os.environ.get(name)]
    report['tls_checks']={};working=[]
    for label,maximum in [('default',None),('tls12',ssl.TLSVersion.TLSv1_2)]:
        context=ssl.create_default_context();context.minimum_version=ssl.TLSVersion.TLSv1_2
        if maximum is not None:context.maximum_version=maximum
        try:
            with socket.create_connection((host,443),timeout=8) as connection:
                with context.wrap_socket(connection,server_hostname=host) as secure:
                    report['tls_checks'][label]=secure.version();working.append(label)
        except (OSError,ssl.SSLError) as error:report['tls_checks'][label]=type(error).__name__+': '+str(error)
    if working:
        source=Source()
        try:
            if 'default' not in working:source.session.mount('https://',TLS12Adapter())
            # One public schedule read, without collector retries or disabled verification.
            payload=source._request_once('service=TrainRunningMob&subService=GetTrainSchedule&trainNo=12617&startDate=')
            train=normalize({'source':'NTES','fetched_at':utcnow(),'payload':payload})
            report['schedule']={'number':train['number'],'stops':len(train['stops']),'valid_to':train['valid_to']}
        except Exception as error:report['source_error']=type(error).__name__+': '+str(error)
        finally:source.close()
    else:report['schedule']='not requested: neither verified TLS handshake completed'
    print(json.dumps(report))

if __name__=='__main__':main()
