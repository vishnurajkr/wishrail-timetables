"""Merge the local collector's public checkpoint without extracting arbitrary files."""
import argparse
import datetime as dt
import json
from pathlib import Path, PurePosixPath
import tarfile
from .pipeline import UTC, parse_timestamp, write_json
from .shared_collector import fresh_record


def merge_checkpoint(archive, root, now=None):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    now=now or dt.datetime.now(UTC)
    state_path=root/'state.json'
    state=json.loads(state_path.read_text()) if state_path.exists() else {}
    restored=0;local_state=None
    with tarfile.open(archive,'r:gz') as source:
        for member in source:
            path=PurePosixPath(member.name)
            if not member.isfile() or member.size>4*1024*1024:continue
            if path.parts==('local-cache','state.json'):
                local_state=json.load(source.extractfile(member));continue
            if len(path.parts)!=3 or path.parts[:2]!=('local-cache','records'):continue
            name=path.name
            if len(name)!=10 or not name[:5].isdigit() or name[5:]!='.json':continue
            target=root/'records'/name
            # Validate in a private temporary file before replacing a known revision.
            import tempfile
            with tempfile.TemporaryDirectory(dir=root) as temp:
                incoming=Path(temp)/name
                incoming.write_bytes(source.extractfile(member).read())
                record=fresh_record(incoming,now)
                if record is None or str(record['payload'].get('TrainNumber'))!=name[:5]:continue
                previous=fresh_record(target,now)
                if previous and parse_timestamp(previous['fetched_at'])>=parse_timestamp(record['fetched_at']):continue
                write_json(target,record);restored+=1
                state.setdefault('retry_after',{}).pop(name[:5],None)
    if local_state:
        directory=local_state.get('directory')
        existing=state.get('directory')
        if directory and (not existing or parse_timestamp(directory['started_at'])>parse_timestamp(existing['started_at'])):
            state['directory']=directory
        for number,receipt in local_state.get('source_unavailable',{}).items():
            previous=state.setdefault('source_unavailable',{}).get(number)
            if previous is None or parse_timestamp(receipt['verified_at'])>parse_timestamp(previous['verified_at']):
                state['source_unavailable'][number]=receipt
    write_json(state_path,state)
    return restored


def main():
    parser=argparse.ArgumentParser();parser.add_argument('archive');parser.add_argument('root')
    args=parser.parse_args()
    print(f'Restored {merge_checkpoint(args.archive,args.root)} newer validated local-collector timetables.')

if __name__=='__main__':main()
