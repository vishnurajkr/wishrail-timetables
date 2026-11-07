import datetime as dt
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from .checkpoint_restore import merge_checkpoint
from .pipeline import UTC, write_json
from .test_pipeline import record

class CheckpointRestoreTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.now=dt.datetime(2026,10,23,tzinfo=UTC)
    def tearDown(self):self.temp.cleanup()
    def archive(self,items):
        path=self.root/'local.tar.gz'
        with tarfile.open(path,'w:gz') as archive:
            for name,value in items.items():
                content=json.dumps(value).encode();info=tarfile.TarInfo(name);info.size=len(content)
                archive.addfile(info,io.BytesIO(content))
        return path
    def test_newer_valid_local_records_replace_old_ci_cache_without_resetting_publication(self):
        cache=self.root/'cache';old=record();old['fetched_at']=(self.now-dt.timedelta(days=8)).isoformat()
        write_json(cache/'records/10001.json',old)
        write_json(cache/'state.json',{'published_fingerprint':'ci-receipt','retry_after':{'10001':'later'}})
        archive=self.archive({'local-cache/records/10001.json':record(),
            'local-cache/state.json':{'directory':{'started_at':self.now.isoformat(),'trains':{'10001':{}},'complete':True},'published_fingerprint':'local-receipt'}})
        self.assertEqual(merge_checkpoint(archive,cache,self.now),1)
        state=json.loads((cache/'state.json').read_text())
        self.assertEqual(state['published_fingerprint'],'ci-receipt')
        self.assertNotIn('10001',state['retry_after'])
        self.assertEqual(state['directory']['trains'],{'10001':{}})
    def test_older_record_cannot_replace_newer_ci_revision(self):
        cache=self.root/'cache';new=record();new['fetched_at']=self.now.isoformat()
        write_json(cache/'records/10001.json',new)
        self.assertEqual(merge_checkpoint(self.archive({'local-cache/records/10001.json':record()}),cache,self.now),0)
        self.assertEqual(json.loads((cache/'records/10001.json').read_text()),new)
    def test_expired_future_mismatched_and_traversal_entries_are_not_imported(self):
        expired=record();expired['fetched_at']=(self.now-dt.timedelta(days=8)).isoformat()
        future=record('10002');future['fetched_at']=(self.now+dt.timedelta(days=1)).isoformat()
        archive=self.archive({'local-cache/records/10001.json':expired,'local-cache/records/10002.json':future,
            'local-cache/records/10003.json':record('99999'),'local-cache/records/../../escape.json':record()})
        cache=self.root/'cache';self.assertEqual(merge_checkpoint(archive,cache,self.now),0)
        self.assertFalse((self.root/'escape.json').exists())
        self.assertEqual(list(cache.glob('records/*.json')),[])
    def test_older_directory_and_receipts_do_not_overwrite_newer_evidence(self):
        cache=self.root/'cache'
        latest={'directory':{'started_at':self.now.isoformat(),'trains':{'10001':{}}},'source_unavailable':{'10001':{'kind':'invalid_train','verified_at':self.now.isoformat()}}}
        write_json(cache/'state.json',latest)
        old=(self.now-dt.timedelta(days=1)).isoformat()
        archive=self.archive({'local-cache/state.json':{'directory':{'started_at':old,'trains':{}},'source_unavailable':{'10001':{'kind':'no_timetable','verified_at':old}}}})
        merge_checkpoint(archive,cache,self.now)
        self.assertEqual(json.loads((cache/'state.json').read_text()),latest)

if __name__=='__main__':unittest.main()
