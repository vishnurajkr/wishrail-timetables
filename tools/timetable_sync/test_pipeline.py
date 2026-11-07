import base64
import contextlib
import copy
import datetime as dt
import gzip
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from tools.timetable_sync.pipeline import build, collect, discover, normalize, publish, running_days, calendar_is_current, UTC


def record(number='10001'):
    return dict(train_number=number,source='NTES',fetched_at='2026-10-22T12:00:00+00:00',payload=dict(
        TrainNumber=number,TrainName='Fixture Express',DaysOfRun='Mon,Wed,Fri',ValidFrom='21-Oct-2026',
        vStartDateList=['21-Oct-2026','23-Oct-2026','26-Oct-2026'],
        stations=[dict(StationCode='AAA',StationName='Alpha',STA='',STD='23:00',Day=1,Distance='0',Sr=1),
                  dict(StationCode='BBB',StationName='Beta',STA='01:00',STD='',Day=2,Distance='100',Sr=2)]))


def base_database(path):
    db=sqlite3.connect(path)
    db.executescript('''
      CREATE TABLE stations(code TEXT PRIMARY KEY,name TEXT NOT NULL,state TEXT,zone TEXT,address TEXT,
        latitude REAL,longitude REAL,train_count INTEGER DEFAULT 0);
      CREATE TABLE trains(number TEXT PRIMARY KEY,name TEXT NOT NULL,type TEXT,classes TEXT,running_days TEXT,
        from_station_code TEXT,from_station_name TEXT,to_station_code TEXT,to_station_name TEXT,departure TEXT,arrival TEXT,
        distance INTEGER,duration_h INTEGER,duration_m INTEGER,sleeper INTEGER,third_ac INTEGER,second_ac INTEGER,
        first_ac INTEGER,first_class INTEGER,chair_car INTEGER);
      CREATE TABLE schedules(id INTEGER PRIMARY KEY,train_number TEXT,train_name TEXT,station_code TEXT,
        station_name TEXT,arrival TEXT,departure TEXT,day INTEGER,stop_sequence INTEGER);
      CREATE TABLE station_connections(from_station TEXT,to_station TEXT,train_number TEXT,from_departure TEXT,
        to_arrival TEXT,from_day INTEGER,to_day INTEGER,from_seq INTEGER,to_seq INTEGER,
        PRIMARY KEY(from_station,to_station,train_number));
      CREATE INDEX idx_train_seq ON schedules(train_number,stop_sequence);
      INSERT INTO trains(number,name,running_days) VALUES('99999','Legacy','1111111');
      INSERT INTO stations(code,name) VALUES('OLD','Old station');
    ''')
    db.commit();db.close()


class NormalizeTests(unittest.TestCase):
    def test_weekdays_never_guessed(self):
        self.assertEqual(running_days('Tue,Thu,Sun'),'0101001')
        self.assertIsNone(running_days('0101010'))
        self.assertIsNone(running_days('unknown'))

    def test_source_date_horizon_and_effective_date(self):
        train=normalize(record())
        self.assertEqual(train['valid_from'],'2026-10-21')
        self.assertEqual(train['valid_to'],'2026-10-26')
        self.assertEqual(train['calendar_mode'],'dates')
        self.assertIsNone(train['stops'][0]['arrival'])
        self.assertIsNone(train['stops'][-1]['departure'])

    def test_unknown_calendar_is_rejected(self):
        data=record();data['payload'].pop('vStartDateList');data['payload']['DaysOfRun']='unknown'
        with self.assertRaisesRegex(ValueError,'bounded operating calendar'):normalize(data)

    def test_overnight_halt_uses_departure_day_without_losing_the_train(self):
        item=record()
        item['payload']['stations']=[
            dict(StationCode='AAA',STA='',STD='22:00',Day=1,Distance='0'),
            dict(StationCode='JJJ',STA='23:55',STD='00:05',Day=2,Distance='50'),
            dict(StationCode='BBB',STA='01:00',STD='',Day=2,Distance='100')]
        parsed=normalize(item)
        self.assertEqual(len(parsed['stops']),3)
        self.assertEqual(parsed['stops'][1]['day'],2)
        self.assertEqual(parsed['stops'][1]['arrival'],'23:55:00')
        item['payload']['stations'][1]['Day']=1
        with self.assertRaisesRegex(ValueError,'arrival day'):normalize(item)

    def test_calendar_includes_later_journey_days_and_uses_indian_date(self):
        train=normalize(record())
        train['valid_to']='2026-10-22'
        self.assertTrue(calendar_is_current(train,dt.datetime(2026,10,23,0,tzinfo=UTC)))
        self.assertFalse(calendar_is_current(train,dt.datetime(2026,10,23,20,tzinfo=UTC)))

    def test_wrong_midnight_day_is_rejected(self):
        data=record();data['payload']['stations'][1]['Day']=1
        with self.assertRaisesRegex(ValueError,'nonmonotonic'):normalize(data)

    def test_zero_minute_halt_is_kept(self):
        data=record();data['payload']['stations'].insert(1,dict(StationCode='JJJ',STA='23:30',STD='23:30',Day=1,Distance='50'))
        self.assertEqual(len(normalize(data)['stops']),3)

    def test_duplicated_complete_route_is_collapsed(self):
        data=record();other=copy.deepcopy(data['payload']['stations'])
        for row in other:row['Sr']+=2
        data['payload']['stations']+=other
        self.assertEqual(len(normalize(data)['stops']),2)

    def test_leading_zero_number_is_preserved(self):
        self.assertEqual(normalize(record('01001'))['number'],'01001')


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.work=tempfile.TemporaryDirectory();self.root=Path(self.work.name)
        self.base=self.root/'base.db';base_database(self.base)
        self.records=self.root/'records';self.records.mkdir()
        (self.records/'10001.json').write_text(json.dumps(record()))

    def tearDown(self):self.work.cleanup()

    def test_older_cache_cannot_overwrite_newer_published_timetable(self):
        fresh=record();fresh['fetched_at']='2026-10-23T12:00:00Z'
        fresh['payload']['TrainName']='Newer timetable'
        (self.records/'10001.json').write_text(json.dumps(fresh))
        first,_=build(self.base,self.records,self.root/'fresh','fresh')
        (self.records/'10001.json').write_text(json.dumps(record()))
        second,_=build(first,self.records,self.root/'older','older')
        with sqlite3.connect(second) as db:
            self.assertEqual(db.execute("SELECT name FROM trains WHERE number='10001'").fetchone()[0],'Newer timetable')
            self.assertEqual(db.execute("SELECT verified_at FROM timetable_calendars WHERE train_number='10001'").fetchone()[0],fresh['fetched_at'])

    def test_complete_census_removes_withdrawn_base_trains(self):
        roster={str(n):{} for n in range(10000,11000)}
        for number in roster:
            (self.records/f'{number}.json').write_text(json.dumps(record(number)))
        evidence=self.root/'roster.json'
        evidence.write_text(json.dumps(dict(complete=True,trains=roster,
            checked_prefixes={f'{i:03}':{} for i in range(1000)})))
        path,meta=build(self.base,self.records,self.root/'national','fixture-national','national',evidence)
        self.assertEqual(meta['verified_trains'],1000)
        self.assertEqual(meta['legacy_trains'],0)
        with sqlite3.connect(path) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM trains WHERE number='99999'").fetchone()[0],0)
        (self.records/'10999.json').unlink()
        with self.assertRaisesRegex(ValueError,'roster differ'):
            build(self.base,self.records,self.root/'incomplete','fixture-missing','national',evidence)

    def test_build_preserves_base_file_but_removes_legacy_from_snapshot(self):
        before=self.base.read_bytes()
        path,meta=build(self.base,self.records,self.root/'out','fixture-pilot')
        self.assertEqual(before,self.base.read_bytes())
        self.assertEqual(meta['verified_trains'],1)
        self.assertEqual(meta['legacy_trains'],0)
        db=sqlite3.connect(path)
        self.assertEqual(db.execute('SELECT count(*) FROM schedules').fetchone()[0],2)
        self.assertEqual(db.execute('SELECT distance_km FROM schedules ORDER BY stop_sequence DESC LIMIT 1').fetchone()[0],100)
        self.assertEqual(db.execute('SELECT count(*) FROM timetable_dates').fetchone()[0],3)
        self.assertEqual(gzip.decompress(path.with_suffix('.db.gz').read_bytes()),path.read_bytes())
        db.close()
        with self.assertRaisesRegex(ValueError,'already exists'):build(self.base,self.records,self.root/'out','fixture-pilot')

    def test_no_empty_or_unverified_national_release(self):
        empty=self.root/'empty';empty.mkdir()
        with self.assertRaisesRegex(ValueError,'No verified'):build(self.base,empty,self.root/'out','empty')
        with self.assertRaisesRegex(ValueError,'discovery'):build(self.base,self.records,self.root/'out','national','national')

    def test_collection_failure_preserves_cache(self):
        class FailedClient:
            def schedule(self,_):raise RuntimeError('HTTP 429')
        before=(self.records/'10001.json').read_bytes()
        with contextlib.redirect_stdout(io.StringIO()):
            report=collect(['10001','10002'],self.records,max_age_hours=0,client=FailedClient())
        self.assertIn('10001',report['failures'])
        self.assertEqual(before,(self.records/'10001.json').read_bytes())
        self.assertFalse((self.records/'10002.json').exists())

    def test_discovery_has_explicit_request_budget(self):
        class Client:
            def search(self,prefix):return {'Trains':[{'TrainNumber':prefix+'00','TrainName':'Test'}]}
        result=discover(['100','101'],self.root/'roster.json',max_requests=1,client=Client())
        self.assertFalse(result['complete'])
        self.assertEqual(result['remaining_prefixes'],['101'])

    def test_signed_manifest_matches_database(self):
        from Crypto.PublicKey import RSA
        from Crypto.Hash import SHA256
        from Crypto.Signature import pkcs1_15
        path,meta=build(self.base,self.records,self.root/'out','signed')
        key=RSA.generate(2048);private=self.root/'test.private.pem';private.write_bytes(key.export_key())
        output=self.root/'manifest.json'
        with contextlib.redirect_stdout(io.StringIO()):publish(path,meta,'https://example.invalid/timetables',private,output)
        envelope=json.loads(output.read_text());payload=base64.b64decode(envelope['payload'])
        pkcs1_15.new(key.public_key()).verify(SHA256.new(payload),base64.b64decode(envelope['signature']))
        self.assertEqual(json.loads(payload)['release_id'],'signed')


if __name__=='__main__':unittest.main()
