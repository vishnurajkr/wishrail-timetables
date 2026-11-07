import contextlib
import datetime as dt
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from .pipeline import UTC, write_json
from .shared_collector import Source, SourceUnavailable, TransientSourceFailure, collect_shared, confirmed_invalid, fresh_record, prepare_release
from .test_pipeline import base_database, record

class SharedCollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.base=self.root/'seed.db';base_database(self.base)
        self.now=dt.datetime(2026,10,23,tzinfo=UTC)
    def tearDown(self):self.temp.cleanup()
    def test_wire_matches_reviewed_cross_language_fixture(self):
        fixture=json.loads(Path('test/data/ntes_wire.json').read_text())
        self.assertEqual(Source.encode(fixture['query']),fixture['encoded_query'])
        self.assertEqual(Source.decode(fixture['encoded_response']),fixture['response_payload'])
    def test_transport_timeouts_back_off_but_http_refusals_are_not_retried(self):
        from unittest.mock import patch
        import requests
        source=Source()
        try:
            with patch.object(source,'_request_once',side_effect=[requests.Timeout(),requests.Timeout(),{'Trains':[]}]) as request, patch('tools.timetable_sync.shared_collector.time.sleep') as sleep:
                self.assertEqual(source.search('100'),[])
                self.assertEqual(request.call_count,3)
                self.assertEqual([call.args[0] for call in sleep.call_args_list],[5,25])
            with patch.object(source,'_request_once',side_effect=SourceUnavailable('HTTP 429')) as request:
                with self.assertRaises(SourceUnavailable):source.search('100')
                self.assertEqual(request.call_count,1)
        finally:source.close()
    def test_only_recent_explicit_invalid_train_responses_can_resolve_a_roster_entry(self):
        state={'source_unavailable':{
            '10001':{'kind':'invalid_train','verified_at':self.now.isoformat()},
            '10002':{'kind':'no_timetable','verified_at':self.now.isoformat()},
            '10003':{'kind':'invalid_train','verified_at':(self.now-dt.timedelta(days=8)).isoformat()}}}
        self.assertEqual(set(confirmed_invalid(state,self.now)),{'10001'})
    def test_full_available_roster_has_auditable_invalid_entries_and_unknowns_block_national(self):
        from Crypto.PublicKey import RSA
        state=self.root/'cache';(state/'records').mkdir(parents=True)
        current=dt.datetime.now(UTC);today=current.date()
        roster={str(n):{} for n in range(10000,11000)}
        for number in roster:
            item=record(number);item['fetched_at']=current.isoformat()
            item['payload']['ValidFrom']=today.isoformat()
            item['payload']['vStartDateList']=[today.isoformat(),(today+dt.timedelta(days=2)).isoformat()]
            write_json(state/'records'/f'{number}.json',item)
        roster['99999']={}
        checkpoint={'directory':{'started_at':current.isoformat(),'remaining':[],
            'checked':{f'{i:03}':{} for i in range(1000)},'trains':roster,'complete':True},
            'source_unavailable':{'99999':{'kind':'invalid_train','reason':'Invalid Train No. 99999 !',
                'verified_at':current.isoformat(),'source':'NTES'}}}
        write_json(state/'state.json',checkpoint)
        key=self.root/'key.pem';key.write_bytes(RSA.generate(2048).export_key())
        with contextlib.redirect_stdout(io.StringIO()):
            result=prepare_release(state,self.base,self.root/'public','complete-test','https://example.org/data',key)
        self.assertEqual(result[1]['coverage'],'national')
        self.assertEqual(result[1]['verified_trains'],1000)
        self.assertEqual(result[1]['source_invalid_trains'],1)
        evidence=json.loads((self.root/'public/complete-test.discovery.json').read_text())
        self.assertIn('99999',evidence['excluded_invalid_trains'])
        checkpoint['directory']['trains']['11000']={}
        checkpoint['source_unavailable']['11000']={'kind':'no_timetable','verified_at':current.isoformat()}
        write_json(state/'state.json',checkpoint)
        with contextlib.redirect_stdout(io.StringIO()):
            result=prepare_release(state,self.base,self.root/'partial','unknown-test','https://example.org/data',key)
        self.assertEqual(result[1]['coverage'],'pilot')

    def test_transient_pause_is_short_and_explicit_retry_after_is_honored(self):
        class BrokenTransport:
            def search(self,prefix):raise TransientSourceFailure('Unreadable response')
        cache=self.root/'transient'
        collect_shared(cache,self.base,source=BrokenTransport(),now=self.now,prefixes=['100'])
        state=json.loads((cache/'state.json').read_text())
        self.assertEqual(dt.datetime.fromisoformat(state['blocked_until'])-self.now,dt.timedelta(minutes=10))
        class Limited:
            def search(self,prefix):raise SourceUnavailable('HTTP 429',retry_after=86400)
        cache=self.root/'limited'
        collect_shared(cache,self.base,source=Limited(),now=self.now,prefixes=['100'])
        state=json.loads((cache/'state.json').read_text())
        self.assertEqual(dt.datetime.fromisoformat(state['blocked_until'])-self.now,dt.timedelta(days=1))

    def test_checkpoint_survives_source_refusal_and_resumes(self):
        class Fake:
            def search(self,prefix):raise SourceUnavailable('HTTP 429')
        state=self.root/'cache'
        report=collect_shared(state,self.base,source=Fake(),now=self.now,prefixes=['100'])
        self.assertEqual(report['status'],'paused')
        checkpoint=json.loads((state/'state.json').read_text())
        self.assertEqual(checkpoint['directory']['remaining'],['100'])
        self.assertEqual(collect_shared(state,self.base,source=Fake(),now=self.now)['status'],'cooldown')
        self.assertEqual(json.loads((state/'report.json').read_text())['status'],'cooldown')
        tick=[0]
        class Resume:
            def search(self,prefix):tick[0]+=1;return [{'TrainNumber':'10001'}]
            def schedule(self,number):tick[0]+=1;return record(number)['payload']
        report=collect_shared(state,self.base,seconds=5,source=Resume(),clock=lambda:tick[0],
                             now=self.now+dt.timedelta(hours=7),prefixes=['100'])
        self.assertTrue(report['directory_complete'])
        self.assertTrue((state/'records/10001.json').exists())
    def test_capped_prefix_is_refined_and_no_stops_are_dropped(self):
        tick=[0]
        class Fake:
            def search(self,prefix):
                tick[0]+=1
                return [{'TrainNumber':f'100{i:02}'} for i in range(60)] if prefix=='100' else []
            def schedule(self,number):tick[0]+=1;return record(number)['payload']
        state=self.root/'cache'
        collect_shared(state,self.base,seconds=1,source=Fake(),clock=lambda:tick[0],now=self.now,prefixes=['100'])
        directory=json.loads((state/'state.json').read_text())['directory']
        self.assertEqual(directory['remaining'],[f'100{i}' for i in range(10)])
        self.assertEqual(len(directory['trains']),60)
    def test_expired_source_calendar_is_quarantined_without_a_refetch_loop(self):
        ticks=[0];requests=[]
        class Expired:
            def search(self,prefix):ticks[0]+=1;return [{'TrainNumber':'10001'}]
            def schedule(self,number):requests.append(number);ticks[0]+=1;return record(number)['payload']
        cache=self.root/'expired'
        report=collect_shared(cache,self.base,seconds=100,source=Expired(),clock=lambda:ticks[0],
            now=self.now+dt.timedelta(days=30),prefixes=['100'])
        self.assertEqual(requests,['10001'])
        self.assertEqual(report['status'],'waiting')
        self.assertEqual(report['downloaded'],0)
        self.assertTrue((cache/'raw/10001.json').exists())
        self.assertFalse((cache/'records/10001.json').exists())

    def test_budget_expiring_before_fetch_is_not_unavailable_wait(self):
        cache=self.root/'boundary'
        write_json(cache/'state.json',{'directory':{'started_at':self.now.isoformat(),
            'complete':True,'remaining':[],'checked':{},'trains':{'10001':{}}}})
        ticks=iter([0,0,0,2,2,2])
        class Fake:
            def schedule(self,number):raise AssertionError('Budget already exhausted')
        report=collect_shared(cache,self.base,seconds=1,source=Fake(),clock=lambda:next(ticks,2),now=self.now)
        self.assertEqual(report['status'],'budget')

    def test_dart_utc_timestamp_suffix_is_reusable_on_python_310(self):
        path=self.root/'dart-record.json';item=record();item['fetched_at']='2026-10-23T00:00:00Z'
        write_json(path,item)
        self.assertIsNotNone(fresh_record(path,self.now))

    def test_collector_refetches_a_valid_schedule_after_24_hours(self):
        cache=self.root/'daily-cache';calls=[]
        old=record();old['fetched_at']=(self.now-dt.timedelta(hours=25)).isoformat()
        write_json(cache/'records/10001.json',old)
        write_json(cache/'state.json',{'directory':{'started_at':self.now.isoformat(),
            'complete':True,'remaining':[],'checked':{'100':{}},'trains':{'10001':{}}}})
        class Fake:
            def search(self,prefix):raise AssertionError('Completed current directory should be reused')
            def schedule(self,number):calls.append(number);return record(number)['payload']
        report=collect_shared(cache,self.base,seconds=5,source=Fake(),now=self.now)
        self.assertEqual(calls,['10001'])
        self.assertEqual(report['downloaded'],1)
        self.assertEqual(report['status'],'complete')

    def test_daily_refresh_threshold_preserves_older_usable_records(self):
        path=self.root/'daily.json';item=record();item['fetched_at']=self.now.isoformat()
        write_json(path,item)
        later=self.now+dt.timedelta(hours=25)
        self.assertIsNone(fresh_record(path,later,hours=24))
        self.assertIsNotNone(fresh_record(path,later))

    def test_old_cache_does_not_satisfy_freshness(self):
        path=self.root/'old.json';write_json(path,record())
        self.assertIsNotNone(fresh_record(path,self.now))
        self.assertIsNone(fresh_record(path,self.now+dt.timedelta(days=10)))
    def test_smaller_publisher_cannot_drop_valid_public_revisions_but_invalid_can(self):
        from Crypto.PublicKey import RSA
        from .pipeline import build
        current=dt.datetime.now(UTC);today=current.date()
        cache=self.root/'safe-cache';baseline_records=self.root/'baseline-records'
        for number in ['10001','10002']:
            item=record(number);item['fetched_at']=current.isoformat()
            item['payload']['ValidFrom']=today.isoformat()
            item['payload']['vStartDateList']=[today.isoformat(),(today+dt.timedelta(days=2)).isoformat()]
            write_json(baseline_records/f'{number}.json',item)
            if number=='10001':write_json(cache/'records'/f'{number}.json',item)
        with contextlib.redirect_stdout(io.StringIO()):
            baseline,_=build(self.base,baseline_records,self.root/'baseline','baseline')
        state={'directory':{'started_at':current.isoformat(),'remaining':['101'],
            'checked':{},'trains':{'10001':{},'10002':{}},'complete':False}}
        write_json(cache/'state.json',state)
        key=self.root/'key.pem';key.write_bytes(RSA.generate(2048).export_key())
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertIsNone(prepare_release(cache,baseline,self.root/'smaller','smaller','https://example.org/data',key))
        state['source_unavailable']={'10002':{'kind':'invalid_train','verified_at':current.isoformat()}}
        write_json(cache/'state.json',state)
        # Even a cached prior schedule cannot override recent invalid-train evidence.
        write_json(cache/'records/10002.json',json.loads((baseline_records/'10002.json').read_text()))
        with contextlib.redirect_stdout(io.StringIO()):
            result=prepare_release(cache,baseline,self.root/'clean','clean','https://example.org/data',key)
        with sqlite3.connect(result[0]) as db:
            self.assertEqual(db.execute('SELECT number FROM trains').fetchall(),[('10001',)])

    def test_source_outage_with_expired_cache_defers_without_publishing_stale_data(self):
        cache=self.root/'outage';old=record()
        old['fetched_at']=(dt.datetime.now(UTC)-dt.timedelta(days=8)).isoformat()
        write_json(cache/'records/10001.json',old)
        write_json(cache/'state.json',{'directory':{'trains':{'10001':{}},'complete':False,'checked':{}}})
        with contextlib.redirect_stdout(io.StringIO()):
            result=prepare_release(cache,self.base,self.root/'outage-public','outage','https://example.org/data',self.root/'not-used.pem')
        self.assertIsNone(result)
        self.assertEqual(json.loads((cache/'publication.json').read_text())['status'],'deferred')
        self.assertFalse((self.root/'outage-public/manifest.json').exists())

    def test_partial_release_does_not_claim_national_and_ack_is_separate(self):
        from Crypto.PublicKey import RSA
        state=self.root/'cache';(state/'records').mkdir(parents=True)
        item=record();item['fetched_at']=dt.datetime.now(UTC).isoformat()
        # A live calendar window relative to the test runner's date.
        today=dt.datetime.now(UTC).date()
        item['payload']['ValidFrom']=today.isoformat()
        item['payload']['vStartDateList']=[today.isoformat(),(today+dt.timedelta(days=2)).isoformat()]
        write_json(state/'records/10001.json',item)
        write_json(state/'state.json',{'directory':{'started_at':self.now.isoformat(),'remaining':['101'],
                    'checked':{'100':{}},'trains':{'10001':{}},'complete':False}})
        key=self.root/'key.pem';key.write_bytes(RSA.generate(2048).export_key())
        with contextlib.redirect_stdout(io.StringIO()):
            result=prepare_release(state,self.base,self.root/'public','partial-test','https://example.org/data',key)
        self.assertEqual(result[1]['coverage'],'pilot')
        self.assertEqual(result[1]['legacy_trains'],0)
        with sqlite3.connect(result[0]) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM trains WHERE number='99999'").fetchone()[0],0)
        self.assertTrue((state/'pending_publication.json').exists())
        self.assertNotIn('published_fingerprint',json.loads((state/'state.json').read_text()))

if __name__=='__main__':unittest.main()
