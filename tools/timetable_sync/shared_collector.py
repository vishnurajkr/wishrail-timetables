"""Resumable, paced collector for one shared timetable release channel.

Only public train directory/timetable methods are used. No user API key is needed.
"""
import argparse
import base64
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time

import requests
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from .pipeline import UTC, build, normalize, publish, utcnow, write_json

class SourceUnavailable(Exception):
    pass
class NoTimetable(Exception):
    pass

class Source:
    def __init__(self, interval=2.0):
        self.session = requests.Session()
        self.last = 0
        self.interval = max(1.5, interval)
    @staticmethod
    def encode(message):
        encrypted = AES.new(b'8EA4DB2CC1EB3DC5', AES.MODE_CBC, b'7DC5EB3BB4DB6EA8').encrypt(pad(message.encode(),16))
        prefix = hashlib.md5((message+'645fbc1e56e23365f2f3c204ae0899f6').encode()).hexdigest().upper()
        return prefix+'#'+base64.b64encode(encrypted).hex().upper()
    @staticmethod
    def decode(value):
        value = value.rsplit('#',1)[-1]
        encrypted = base64.b64decode(bytes.fromhex(value).decode())
        return json.loads(unpad(AES.new(b'8EA4DB2CC1EB3DC5',AES.MODE_CBC,b'7DC5EB3BB4DB6EA8').decrypt(encrypted),16))
    def request(self, query, directory=False):
        time.sleep(max(0,self.interval-(time.monotonic()-self.last)))
        self.last = time.monotonic()
        response = self.session.post('https://enquiry.indianrail.gov.in/crisns/AppServAnd',
            json={'jsonIn':self.encode(query)}, headers={'User-Agent':'Dalvik/2.1.0 (Linux; Android 11)'},
            timeout=(15,25),allow_redirects=False)
        if response.status_code!=200:
            raise SourceUnavailable(f'Railway source HTTP {response.status_code}; collection paused')
        if len(response.content)>4*1024*1024: raise SourceUnavailable('Unexpected source response size')
        outer=response.json()
        data=self.decode(outer['jsonIn']) if isinstance(outer.get('jsonIn'),str) else outer
        alert=str(data.get('AlertMsg') or data.get('alertMsg') or '')
        if alert:
            if re.search(r'no (train|schedule)|no match found|invalid train|not found|does not exist',alert,re.I):
                if directory:return {'Trains':[]}
                raise NoTimetable(alert)
            raise SourceUnavailable(alert)
        return data
    def search(self,prefix):
        result=self.request(f'service=TrainRunningMob&subService=FindTrainJson&trainNo={prefix}',directory=True)
        if not isinstance(result.get('Trains'),list):raise SourceUnavailable('Incomplete directory response')
        return result['Trains']
    def schedule(self,number):
        return self.request(f'service=TrainRunningMob&subService=GetTrainSchedule&trainNo={number}&startDate=')
    def close(self):self.session.close()


def fresh_record(path, now, hours=168):
    try:
        record=json.loads(path.read_text())
        normalized=normalize(record)
        if normalized['valid_to'] < now.date().isoformat():return None
        age=now-dt.datetime.fromisoformat(record['fetched_at'])
        return record if dt.timedelta(0)<=age<dt.timedelta(hours=hours) else None
    except (OSError,ValueError,KeyError,TypeError):return None


def collect_shared(root, base, seconds=4800, source=None, clock=time.monotonic, now=None, prefixes=None):
    """Interleave directory and train work; every request is checkpointed."""
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    records=root/'records';records.mkdir(exist_ok=True)
    state_path=root/'state.json'
    state=json.loads(state_path.read_text()) if state_path.exists() else {}
    today=now or dt.datetime.now(UTC)
    deadline=clock()+seconds
    if not state.get('directory') or (state['directory'].get('complete') and
       today-dt.datetime.fromisoformat(state['directory']['started_at'])>dt.timedelta(days=7)):
        state['directory']={'started_at':today.isoformat(),'remaining':prefixes or [f'{i:03}' for i in range(1000)],
                            'checked':{},'trains':{},'complete':False}
    directory=state['directory']; retry=state.setdefault('retry_after',{})
    blocked_until=state.get('blocked_until')
    if blocked_until and today<dt.datetime.fromisoformat(blocked_until):
        report=dict(status='cooldown',downloaded=0,discovered=len(directory['trains']),
                    directory_complete=directory['complete'],blocked_until=blocked_until,finished_at=utcnow())
        write_json(root/'report.json',report)
        return report
    db=sqlite3.connect(base)
    seed={r[0] for r in db.execute('SELECT number FROM trains') if re.fullmatch(r'\d{5}',r[0])};db.close()
    report={'started_at':today.isoformat(),'downloaded':0,'directory_requests':0,'unavailable':{},'status':'budget'}
    try:
        while clock()<deadline:
            advanced=False
            # Short batches provide useful partial releases before the first census ends.
            for _ in range(24):
                if clock()>=deadline or not directory['remaining']:break
                prefix=directory['remaining'][0]
                rows=source.search(prefix)
                for row in rows:
                    number=str(row.get('TrainNumber',''))
                    if re.fullmatch(r'\d{5}',number) and number.startswith(prefix):
                        directory['trains'][number]={'name':row.get('TrainName'),'discovered_at':today.isoformat()}
                if len(rows)>=60 and len(prefix)<5:
                    directory['remaining'].extend(prefix+str(i) for i in range(10))
                directory['remaining'].pop(0)
                directory['checked'][prefix]={'checked_at':today.isoformat(),'count':len(rows)}
                directory['complete']=not directory['remaining']
                report['directory_requests']+=1;advanced=True
                write_json(state_path,state)
            pool=set(directory['trains']) if directory['complete'] else seed|set(directory['trains'])
            pending=[]
            outstanding=0
            for number in pool:
                path=records/f'{number}.json'
                if fresh_record(path,today) is not None:continue
                outstanding+=1
                if number in retry and today<dt.datetime.fromisoformat(retry[number]):continue
                pending.append(number)
            # Missing express services first, then the oldest cached records.
            pending.sort(key=lambda n:(0 if n.startswith(('1','2')) else 1,
                         (records/f'{n}.json').stat().st_mtime if (records/f'{n}.json').exists() else 0,n))
            for number in pending[:48]:
                if clock()>=deadline:break
                try:
                    record={'train_number':number,'source':'NTES','fetched_at':(now or dt.datetime.now(UTC)).isoformat(),
                            'payload':source.schedule(number)}
                    normalize(record)
                    if str(record['payload'].get('TrainNumber'))!=number:raise ValueError('Different train returned')
                    write_json(records/f'{number}.json',record)
                    retry.pop(number,None);report['downloaded']+=1
                except (NoTimetable,ValueError,KeyError,TypeError) as error:
                    retry[number]=(today+dt.timedelta(hours=24)).isoformat()
                    report['unavailable'][number]=str(error)
                advanced=True;write_json(state_path,state)
            if not advanced:
                report['status']='complete' if directory['complete'] and not outstanding else 'waiting'
                break
    except (SourceUnavailable,requests.RequestException) as error:
        report.update(status='paused',reason=str(error))
        state['blocked_until']=(today+dt.timedelta(hours=6)).isoformat()
    finally:
        write_json(state_path,state)
        report.update(discovered=len(directory['trains']),directory_complete=directory['complete'],finished_at=utcnow())
        write_json(root/'report.json',report)
    return report


def prepare_release(root,base,output,release_id,base_url,key):
    root=Path(root);output=Path(output)
    state=json.loads((root/'state.json').read_text())
    roster=state['directory']; now=dt.datetime.now(UTC)
    available={p.stem:p for p in (root/'records').glob('*.json') if fresh_record(p,now) is not None}
    complete=roster['complete'] and len(roster['trains'])>=1000 and all(f'{i:03}' in roster['checked'] for i in range(1000))
    national=complete and set(roster['trains'])<=available.keys()
    usable={n:p for n,p in available.items() if not national or n in roster['trains']}
    if not usable:raise ValueError('No validated recent timetables to publish')
    fingerprint=hashlib.sha256(b''.join(p.read_bytes() for n,p in sorted(usable.items()))+
        json.dumps(roster,sort_keys=True).encode()).hexdigest()
    if state.get('published_fingerprint')==fingerprint:return None
    import tempfile,shutil
    output.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output) as temp:
        records=Path(temp)/'records';records.mkdir()
        for number,path in usable.items():shutil.copyfile(path,records/path.name)
        evidence=Path(temp)/'roster.json'
        write_json(evidence,dict(complete=complete,trains=roster['trains'],checked_prefixes=roster['checked']))
        candidate,metadata=build(base,records,output,release_id,'national' if national else 'pilot',evidence if national else None)
    if not national:
        # "pilot" is the builder's partial-coverage mode, never a national claim.
        pass
    publish(candidate,metadata,base_url,key,output/'manifest.json')
    # Only publication acknowledgement commits this fingerprint; failed uploads retry.
    write_json(root/'pending_publication.json',{'fingerprint':fingerprint,'release_id':release_id})
    return candidate,metadata


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',default='tools/shared_timetable_cache')
    parser.add_argument('--base',default='assets/data/wishrail.db')
    parser.add_argument('--seconds',type=int,default=4800)
    parser.add_argument('--publish-dir')
    parser.add_argument('--release-id')
    parser.add_argument('--base-url')
    parser.add_argument('--key')
    parser.add_argument('--acknowledge',action='store_true')
    args=parser.parse_args()
    if args.acknowledge:
        state=json.loads((Path(args.root)/'state.json').read_text())
        pending=json.loads((Path(args.root)/'pending_publication.json').read_text())
        state['published_fingerprint']=pending['fingerprint'];write_json(Path(args.root)/'state.json',state);return
    source=Source()
    try:print(json.dumps(collect_shared(args.root,args.base,args.seconds,source=source)))
    finally:source.close()
    if args.publish_dir:
        if not all([args.release_id,args.base_url,args.key]):parser.error('Publishing needs release ID, base URL and signing key')
        release=prepare_release(args.root,args.base,args.publish_dir,args.release_id,args.base_url,args.key)
        print('Prepared '+release[0].name if release else 'No changed release')

if __name__=='__main__':main()
