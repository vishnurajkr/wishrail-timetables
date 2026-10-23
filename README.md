# WishRail timetable downloads

The public download feed is enabled at `releases/download/timetable-latest/manifest.json`. Collection status is visible in the Actions run summary. A successful job may publish previously collected data while source access is paused; national freshness is not implied.

This repository contains the shared timetable collector and public timetable data only. WishRail app source is not included.

The scheduled workflow resumes collection every six hours, within an 80-minute budget. The collector targets refetching schedules after 24 hours and renewing a completed directory daily. This is a target, dependent on successful source access and available collection time. Older usable records may be retained for up to seven days with their real verification timestamps while refreshes continue. Source requests are sequential and paced. Refusals pause collection for six hours. Completed train records and directory sections are checkpointed. A national label requires a complete directory and a recent validated schedule for every discovered train. Otherwise the release is explicitly partial.

## Enable publication

1. Create the public repository `vishnurajkr/wishrail-timetables` and push these prepared files.
2. Set the Actions secret `TIMETABLE_SIGNING_KEY` to the private signing key prepared locally in WishRail. Never commit it or upload it as a release asset.
3. Run the Collect and publish timetables workflow manually. Use a budget of 0 for the initial partial seed, or 4800 to collect more schedules first.
4. The app checks `releases/download/timetable-latest/manifest.json`. The manifest is uploaded after its immutable database asset.

A first national collection takes multiple runs. The initial seed has 32 verified timetables and 5,180 legacy records; it is not a current national timetable. Public endpoints are an unofficial integration and can change. Source fetch times and calendar limits remain visible.
