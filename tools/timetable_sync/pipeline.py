"""Collect public NTES schedules and create compatible, signed offline snapshots.

Network collection is explicit. Building always uses a copy, never the source DB.
"""
import argparse
import base64
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import sqlite3
import tempfile
import time

UTC = dt.timezone.utc
SCHEMA = 2


def utcnow():
    return dt.datetime.now(UTC).isoformat()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def iso_date(value):
    if not value:
        return None
    for fmt in ('%Y-%m-%d', '%d-%b-%Y'):
        try:
            return dt.datetime.strptime(str(value), fmt).date().isoformat()
        except ValueError:
            pass
    raise ValueError('Unrecognized source date: ' + str(value))


def running_days(value):
    text = str(value or '').strip().lower()
    if text == 'daily':
        return '1111111'
    # Do not guess the ordering of undocumented numeric masks.
    tokens = re.split(r'[,\s/;]+', text)
    days = ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')
    if not tokens or not all(len(t) >= 3 and t[:3] in days for t in tokens):
        return None
    return ''.join('1' if any(t[:3] == d for t in tokens) else '0' for d in days)


def clock(value):
    value = str(value or '').strip()
    if value.lower() in ('', 'source', 'destination', 'none', '--', 'dstn'):
        return None
    if not re.fullmatch(r'\d{1,2}:\d{2}(:\d{2})?', value):
        raise ValueError('Invalid schedule time: ' + value)
    h, m = map(int, value.split(':')[:2])
    if h > 23 or m > 59:
        raise ValueError('Invalid schedule time: ' + value)
    return f'{h:02}:{m:02}:00'


def normalize(record):
    payload = record['payload']
    number = str(payload.get('TrainNumber', record.get('train_number', '')))
    if not re.fullmatch(r'\d{5}', number):
        raise ValueError('Expected a five digit train number')
    stops = []
    previous = -1
    distance = -1
    source_stops = payload.get('stations', [])
    # NTES occasionally repeats the entire route with renumbered Sr values.
    # Collapse only identical complete blocks; do not remove repeated stations.
    comparable = [{k: v for k, v in row.items() if k != 'Sr'} for row in source_stops]
    for size in range(2, len(comparable) // 2 + 1):
        if len(comparable) % size == 0 and comparable == comparable[:size] * (len(comparable) // size):
            source_stops = source_stops[:size]
            break
    for seq, row in enumerate(source_stops, 1):
        code = str(row.get('StationCode', '')).strip().upper()
        if not re.fullmatch(r'[A-Z0-9-]{1,12}', code):
            raise ValueError(f'{number}: invalid station code')
        day = int(row['Day'])
        if day < 1 or day > 15:
            raise ValueError(f'{number}: invalid day')
        arrival = None if seq == 1 else clock(row.get('STA'))
        departure = None if seq == len(source_stops) else clock(row.get('STD'))
        arrival_day=day
        if arrival and departure and arrival>departure:
            arrival_day-=1
        if arrival_day<1:raise ValueError(f'{number}: invalid arrival day')
        for value,event_day in ((arrival,arrival_day),(departure,day)):
            if value:
                h, m = map(int, value.split(':')[:2])
                minutes = (event_day - 1) * 1440 + h * 60 + m
                if minutes < previous:
                    raise ValueError(f'{number}: nonmonotonic times at {code}')
                previous = minutes
        km = int(float(row['Distance'])) if row.get('Distance') not in (None, '') else None
        if km is not None:
            if km < distance:
                raise ValueError(f'{number}: decreasing distance')
            distance = km
        stops.append(dict(station_code=code, station_name=row.get('StationName') or code,
                          arrival=arrival, departure=departure, day=day,
                          stop_sequence=seq, distance_km=km))
    if len(stops) < 2 or not stops[0]['departure'] or not stops[-1]['arrival']:
        raise ValueError(f'{number}: incomplete schedule')
    start_dates = sorted(set(iso_date(d) for d in payload.get('vStartDateList', [])))
    valid_from = iso_date(payload.get('ValidFrom'))
    # Finite operating-date lists remain finite. Absence outside this window is unknown.
    calendar_start = max(start_dates[0], valid_from or start_dates[0]) if start_dates else valid_from
    calendar_end = start_dates[-1] if start_dates else iso_date(payload.get('ValidTo'))
    mask = running_days(payload.get('DaysOfRun'))
    if not start_dates and (mask is None or not calendar_start or not calendar_end):
        raise ValueError(f'{number}: no bounded operating calendar')
    if not calendar_start or not calendar_end or calendar_start > calendar_end:
        raise ValueError(f'{number}: invalid calendar horizon')
    return dict(number=number, name=payload.get('TrainName') or number,
                type=payload.get('TrainType'), classes=payload.get('ClassOfTravel'),
                running_days=mask or '0000000', valid_from=calendar_start,
                valid_to=calendar_end, calendar_mode='dates' if start_dates else 'weekly',
                dates=[d for d in start_dates if calendar_start <= d <= calendar_end],
                fetched_at=record['fetched_at'], source=record.get('source', 'NTES'), stops=stops)


def collect(numbers, output, max_age_hours=24, pause=2.0, client=None):
    if client is None:
        from ntes import NTESClient
        client = NTESClient(timeout=20, retries=0)
        # Fail on refusal instead of treating an HTTP error page as timetable data.
        client.session.hooks['response'] = [lambda r, *a, **k: r.raise_for_status()]
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    report = dict(started_at=utcnow(), success=[], cached=[], failures={})
    for index, number in enumerate(numbers):
        if not re.fullmatch(r'\d{5}', number):
            report['failures'][number] = 'Not a five-digit passenger service number'
            continue
        path = output / f'{number}.json'
        if path.exists():
            old = json.loads(path.read_text())
            age = dt.datetime.now(UTC) - dt.datetime.fromisoformat(old['fetched_at'])
            if dt.timedelta(0) <= age < dt.timedelta(hours=max_age_hours):
                report['cached'].append(number)
                continue
        if index:
            time.sleep(max(pause, 1.5) + random.uniform(0, 0.4))
        try:
            record = dict(train_number=number, source='NTES', fetched_at=utcnow(), payload=client.schedule(number))
            if str(record['payload'].get('TrainNumber')) != number:
                raise ValueError('Source returned a different train number')
            # Preserve the upstream response even when normalization rejects it.
            write_json(output / 'raw' / f'{number}.json', record)
            try:
                normalize(record)
            except ValueError as exc:
                report['failures'][number] = str(exc)
                print(f'{number}: quarantined: {exc}', flush=True)
                continue
            write_json(path, record)
            report['success'].append(number)
            print(f'{number}: verified', flush=True)
        except Exception as exc:
            report['failures'][number] = str(exc)
            print(f'{number}: collection stopped: {exc}', flush=True)
            # Stop conservatively on the first refusal/failure; preserve previous cache.
            break
        finally:
            write_json(output / 'collection_report.json', report)
    report['finished_at'] = utcnow()
    write_json(output / 'collection_report.json', report)
    return report


def discover(prefixes, output, max_requests=50, client=None):
    if client is None:
        from ntes import NTESClient
        client = NTESClient(timeout=20, retries=0)
        client.session.hooks['response'] = [lambda r, *a, **k: r.raise_for_status()]
    path = Path(output)
    state = json.loads(path.read_text()) if path.exists() else {'trains': {}, 'checked_prefixes': {}}
    queue = list(dict.fromkeys(state.get('remaining_prefixes', []) + list(prefixes)))
    requests = 0
    while queue and requests < max_requests:
        prefix = queue.pop(0)
        if not re.fullmatch(r'\d{3,5}', prefix):
            raise ValueError('Discovery accepts three-to-five digit prefixes only')
        checked = state['checked_prefixes'].get(prefix, {}).get('checked_at')
        if checked and dt.datetime.now(UTC) - dt.datetime.fromisoformat(checked) < dt.timedelta(hours=24):
            continue
        if requests:
            time.sleep(2 + random.uniform(0, 0.4))
        result = client.search(prefix)
        requests += 1
        rows = result.get('Trains')
        if not isinstance(rows, list):
            raise ValueError('Unexpected discovery response; preserving previous roster')
        for row in rows:
            number = str(row.get('TrainNumber', ''))
            if re.fullmatch(r'\d{5}', number):
                state['trains'][number] = dict(name=row.get('TrainName'), discovered_at=utcnow())
        state['checked_prefixes'][prefix] = dict(checked_at=utcnow(), count=len(rows))
        # Observed NTES search cap; a full response requires deeper discovery.
        if len(rows) >= 60 and len(prefix) < 5:
            queue.extend(prefix + str(d) for d in range(10))
        state['remaining_prefixes'] = queue
        state['complete'] = not queue
        write_json(path, state)
    state['remaining_prefixes'] = queue
    state['complete'] = not queue
    write_json(path, state)
    return state


def build(base, records, output, release_id, coverage='pilot', roster=None):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,79}', release_id):
        raise ValueError('Invalid release ID')
    record_paths = sorted(Path(records).glob('[0-9]*.json'))
    discovery = None
    if coverage == 'national':
        if roster is None: raise ValueError('National coverage requires complete discovery evidence')
        discovery = json.loads(Path(roster).read_text())
        if (not discovery.get('complete') or len(discovery.get('trains',{})) < 1000 or
            not all(f'{i:03}' in discovery.get('checked_prefixes',{}) for i in range(1000))):
            raise ValueError('National discovery is incomplete or implausibly small')
        record_paths = [path for path in record_paths if path.stem in discovery['trains']]
    if not record_paths:
        raise ValueError('No verified timetables; refusing to create a release')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    target = output / f'{release_id}.db'
    if target.exists() or target.with_suffix('.db.gz').exists():
        raise ValueError('Release already exists; use a new immutable release ID')
    with tempfile.TemporaryDirectory(dir=output) as work:
        candidate = Path(work) / 'candidate.db'
        src = sqlite3.connect(f'file:{Path(base).resolve()}?mode=ro', uri=True)
        db = sqlite3.connect(candidate)
        src.backup(db)
        src.close()
        db.execute('PRAGMA foreign_keys=ON')
        db.executescript('''
          CREATE TABLE IF NOT EXISTS timetable_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS timetable_calendars (
            train_number TEXT PRIMARY KEY REFERENCES trains(number), mode TEXT NOT NULL,
            valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, verified_at TEXT NOT NULL, source TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS timetable_dates (
            train_number TEXT NOT NULL REFERENCES trains(number), source_date TEXT NOT NULL,
            PRIMARY KEY(train_number, source_date));
        ''')
        columns = {r[1] for r in db.execute('PRAGMA table_info(schedules)')}
        if discovery is not None:
            db.execute('CREATE TEMP TABLE active_roster(number TEXT PRIMARY KEY)')
            db.executemany('INSERT INTO active_roster VALUES(?)',[(n,) for n in discovery['trains']])
            for table in ['timetable_dates','timetable_calendars','station_connections','schedules']:
                db.execute(f'DELETE FROM {table} WHERE train_number NOT IN (SELECT number FROM active_roster)')
            db.execute('DELETE FROM trains WHERE number NOT IN (SELECT number FROM active_roster)')
        if 'distance_km' not in columns:
            db.execute('ALTER TABLE schedules ADD COLUMN distance_km INTEGER')
        train_numbers=[]
        for record_path in record_paths:
            train=normalize(json.loads(record_path.read_text()))
            if train['number']!=record_path.stem:raise ValueError('Record filename and train number differ')
            number, stops = train['number'], train['stops']
            train_numbers.append(number)
            previous_revision=db.execute('SELECT verified_at FROM timetable_calendars WHERE train_number=?',(number,)).fetchone()
            if previous_revision and dt.datetime.fromisoformat(previous_revision[0]) > dt.datetime.fromisoformat(train['fetched_at']):
                # Independent publishers must never replace a newer collected revision with an older cache.
                continue
            for stop in stops:
                db.execute('INSERT INTO stations(code,name,train_count) VALUES(?,?,0) '
                           'ON CONFLICT(code) DO UPDATE SET name=excluded.name',
                           (stop['station_code'], stop['station_name']))
            classes = set((train['classes'] or '').split(','))
            def minute(stop, field):
                h, m = map(int, stop[field].split(':')[:2])
                return (stop['day'] - 1) * 1440 + h * 60 + m
            duration = minute(stops[-1], 'arrival') - minute(stops[0], 'departure')
            db.execute('''INSERT INTO trains(number,name) VALUES(?,?) ON CONFLICT(number) DO UPDATE SET name=excluded.name''', (number, train['name']))
            db.execute('''UPDATE trains SET type=?,classes=?,running_days=?,from_station_code=?,from_station_name=?,
              to_station_code=?,to_station_name=?,departure=?,arrival=?,distance=?,sleeper=?,third_ac=?,second_ac=?,first_ac=?,chair_car=? WHERE number=?''',
              (train['type'],train['classes'],train['running_days'],stops[0]['station_code'],stops[0]['station_name'],
               stops[-1]['station_code'],stops[-1]['station_name'],stops[0]['departure'],stops[-1]['arrival'],stops[-1]['distance_km'],
               int('SL' in classes),int('3A' in classes),int('2A' in classes),int('1A' in classes),int('CC' in classes),number))
            db.execute('UPDATE trains SET duration_h=?,duration_m=? WHERE number=?', (duration // 60, duration % 60, number))
            db.execute('DELETE FROM schedules WHERE train_number=?', (number,))
            db.executemany('''INSERT INTO schedules(train_number,train_name,station_code,station_name,arrival,departure,day,stop_sequence,distance_km)
                VALUES(?,?,?,?,?,?,?,?,?)''', [(number,train['name'],s['station_code'],s['station_name'],s['arrival'],s['departure'],s['day'],s['stop_sequence'],s['distance_km']) for s in stops])
            db.execute('INSERT OR REPLACE INTO timetable_calendars VALUES(?,?,?,?,?,?)',
                       (number,train['calendar_mode'],train['valid_from'],train['valid_to'],train['fetched_at'],train['source']))
            db.execute('DELETE FROM timetable_dates WHERE train_number=?', (number,))
            db.executemany('INSERT INTO timetable_dates VALUES(?,?)', [(number,d) for d in train['dates']])
        db.execute('DELETE FROM station_connections')
        db.execute('''INSERT OR IGNORE INTO station_connections
          SELECT a.station_code,b.station_code,a.train_number,a.departure,b.arrival,a.day,CASE WHEN b.arrival>b.departure THEN b.day-1 ELSE b.day END,a.stop_sequence,b.stop_sequence
          FROM schedules a JOIN schedules b ON a.train_number=b.train_number
          WHERE b.stop_sequence=(SELECT MIN(c.stop_sequence) FROM schedules c WHERE c.train_number=a.train_number AND c.stop_sequence>a.stop_sequence)''')
        db.execute('UPDATE stations SET train_count=(SELECT COUNT(DISTINCT train_number) FROM schedules WHERE station_code=stations.code)')
        verified = db.execute('SELECT COUNT(*),MIN(verified_at),MAX(verified_at) FROM timetable_calendars').fetchone()
        total = db.execute('SELECT COUNT(*) FROM trains').fetchone()[0]
        if coverage == 'national':
            if verified[0] != total:
                raise ValueError('National release contains unverified legacy records')
            if roster is None:
                raise ValueError('National coverage requires complete discovery evidence')
            discovery = json.loads(Path(roster).read_text())
            if not discovery.get('complete') or not all(f'{i:03}' in discovery.get('checked_prefixes', {}) for i in range(1000)):
                raise ValueError('National discovery has not covered every number prefix')
            actual = {r[0] for r in db.execute('SELECT number FROM trains')}
            if actual != set(discovery['trains']):
                raise ValueError('Snapshot and discovered active train roster differ')
        horizons = db.execute('SELECT MIN(valid_from),MAX(valid_to) FROM timetable_calendars').fetchone()
        metadata = dict(calendar_from=horizons[0],calendar_to=horizons[1],release_id=release_id,built_at=utcnow(),coverage=coverage,verified_trains=verified[0],
                        total_trains=total,legacy_trains=total-verified[0],oldest_verified_at=verified[1],newest_verified_at=verified[2],
                        schema_version=SCHEMA)
        if discovery is not None:
            metadata.update(discovered_trains=len(discovery['trains'])+len(discovery.get('excluded_invalid_trains',{})),
                source_invalid_trains=len(discovery.get('excluded_invalid_trains',{})),
                discovery_file=release_id+'.discovery.json',discovery_sha256=hashlib.sha256(Path(roster).read_bytes()).hexdigest())
        db.executemany('INSERT OR REPLACE INTO timetable_metadata VALUES(?,?)', [(k,json.dumps(v)) for k,v in metadata.items()])
        db.execute(f'PRAGMA user_version={SCHEMA}')
        db.commit()
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('Database integrity failure')
        # The legacy dataset has preexisting orphan records; no refreshed train may add any.
        for number in train_numbers:
            if db.execute('SELECT COUNT(*) FROM schedules s LEFT JOIN stations st ON s.station_code=st.code WHERE s.train_number=? AND st.code IS NULL',(number,)).fetchone()[0]:
                raise ValueError('Refreshed schedule references missing stations')
        db.execute('PRAGMA journal_mode=DELETE')
        db.close()
        shutil.copyfile(candidate, target)
    gz = target.with_suffix('.db.gz')
    with target.open('rb') as source, gz.open('wb') as raw:
        with gzip.GzipFile(fileobj=raw,mode='wb',mtime=0) as dest:
            shutil.copyfileobj(source,dest)
    write_json(output / f'{release_id}.metadata.json',metadata)
    return target, metadata


def publish(database, metadata, base_url, private_key, output):
    from Crypto.Hash import SHA256
    from Crypto.PublicKey import RSA
    from Crypto.Signature import pkcs1_15
    if not base_url.startswith('https://'):
        raise ValueError('Release distribution requires HTTPS')
    database = Path(database)
    connection = sqlite3.connect(f'file:{database.resolve()}?mode=ro', uri=True)
    try:
        actual_metadata = {k:json.loads(v) for k,v in connection.execute('SELECT key,value FROM timetable_metadata')}
        if actual_metadata != metadata:
            raise ValueError('Metadata does not match the database being published')
    finally:
        connection.close()
    gz = database.with_suffix('.db.gz')
    payload = dict(metadata, format_version=1, min_app_build=7,
                   url=base_url.rstrip('/')+'/'+gz.name,
                   compressed_bytes=gz.stat().st_size, database_bytes=database.stat().st_size,
                   compressed_sha256=hashlib.sha256(gz.read_bytes()).hexdigest(),
                   database_sha256=hashlib.sha256(database.read_bytes()).hexdigest())
    message = json.dumps(payload,sort_keys=True,separators=(',',':')).encode()
    key = RSA.import_key(Path(private_key).read_bytes())
    if not key.has_private() or key.size_in_bits() < 2048:
        raise ValueError('A private RSA key of at least 2048 bits is required')
    signature = pkcs1_15.new(key).sign(SHA256.new(message))
    write_json(output,dict(payload=base64.b64encode(message).decode(),signature=base64.b64encode(signature).decode()))
    public_config = base64.b64encode(json.dumps(dict(n=str(key.n),e=str(key.e))).encode()).decode()
    print('WISHRAIL_UPDATE_PUBLIC_KEY='+public_config)


def bundle(database, assets):
    database = Path(database)
    assets = Path(assets)
    connection = sqlite3.connect(f'file:{database.resolve()}?mode=ro', uri=True)
    try:
        if connection.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('Cannot bundle an invalid database')
        metadata = {k:json.loads(v) for k,v in connection.execute('SELECT key,value FROM timetable_metadata')}
    finally:
        connection.close()
    compressed = database.with_suffix('.db.gz')
    if hashlib.sha256(gzip.decompress(compressed.read_bytes())).digest() != hashlib.sha256(database.read_bytes()).digest():
        raise ValueError('Compressed artifact does not match database')
    assets.mkdir(parents=True, exist_ok=True)
    for name, source in [('wishrail.db',database),('wishrail.db.gz',compressed)]:
        target = assets / name
        if source.resolve() == target.resolve():
            continue
        # Retain originals outside packaged assets for a reversible local rollout.
        backup = database.parent / ('before-bundle-' + name)
        if target.exists() and not backup.exists():
            shutil.copyfile(target, backup)
        temporary = target.with_suffix(target.suffix + '.tmp')
        shutil.copyfile(source, temporary)
        temporary.replace(target)
    write_json(assets / 'bundled_snapshot.json',metadata)
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command',required=True)
    c = sub.add_parser('collect')
    inputs = c.add_mutually_exclusive_group(required=True)
    inputs.add_argument('--trains',help='Comma-separated five digit train numbers')
    inputs.add_argument('--train-file',help='JSON roster produced by discover')
    c.add_argument('--output',default='tools/timetable_cache')
    c.add_argument('--max-age-hours',type=float,default=24)
    c.add_argument('--limit',type=int,default=50)
    d = sub.add_parser('discover')
    d.add_argument('--prefixes',required=True,help='Comma-separated three digit prefixes')
    d.add_argument('--output',default='tools/timetable_cache/roster.json')
    d.add_argument('--max-requests',type=int,default=50)
    b = sub.add_parser('build')
    b.add_argument('--base',default='assets/data/wishrail.db')
    b.add_argument('--records',default='tools/timetable_cache')
    b.add_argument('--output',default='build/timetables')
    b.add_argument('--release-id',required=True)
    b.add_argument('--coverage',choices=['pilot','national'],default='pilot')
    b.add_argument('--roster',help='Complete discovery evidence, required for national coverage')
    pack = sub.add_parser('bundle')
    pack.add_argument('--database',required=True)
    pack.add_argument('--assets',default='assets/data')
    p = sub.add_parser('publish')
    p.add_argument('--database',required=True)
    p.add_argument('--metadata',required=True)
    p.add_argument('--base-url',required=True)
    p.add_argument('--private-key',required=True)
    p.add_argument('--output',required=True)
    args = parser.parse_args()
    if args.command == 'collect':
        if args.limit < 1:
            parser.error('--limit must be positive')
        numbers = list(dict.fromkeys(args.trains.split(','))) if args.trains else sorted(json.loads(Path(args.train_file).read_text())['trains'])
        if args.train_file:
            def cache_age(number):
                cached = Path(args.output) / f'{number}.json'
                if not cached.exists():
                    return ''
                try:
                    return json.loads(cached.read_text()).get('fetched_at', '')
                except (ValueError, OSError):
                    return ''
            numbers.sort(key=cache_age)
        numbers = numbers[:args.limit]
        result = collect(numbers,args.output,args.max_age_hours)
        raise SystemExit(1 if result['failures'] else 0)
    if args.command == 'discover':
        if args.max_requests < 1:
            parser.error('--max-requests must be positive')
        prefixes = [f'{i:03}' for i in range(1000)] if args.prefixes == 'all' else args.prefixes.split(',')
        state = discover(prefixes,args.output,args.max_requests)
        print(json.dumps(dict(discovered=len(state['trains']),complete_for_requested_scope=state['complete'],remaining_prefixes=state['remaining_prefixes']),indent=2))
    if args.command == 'build':
        target,metadata = build(args.base,args.records,args.output,args.release_id,args.coverage,args.roster)
        print(target);print(json.dumps(metadata,indent=2))
    if args.command == 'bundle':
        print(json.dumps(bundle(args.database,args.assets),indent=2))
    if args.command == 'publish':
        publish(args.database,json.loads(Path(args.metadata).read_text()),args.base_url,args.private_key,args.output)


if __name__ == '__main__':
    main()
