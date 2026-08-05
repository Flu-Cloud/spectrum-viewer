@echo off
REM ingest_all_stats.bat -- pull median and mean PSD for all 10 SEA sensors in
REM one command instead of running psd_ingest.py by hand ~22 times.
REM
REM     cd C:\Users\pipyt\ATLAS
REM     ingest_all_stats.bat
REM
REM All the logic lives in ingest\ingest_all_stats.py (tested end to end
REM against real Box CSVs before delivery); this is just the one-command
REM entry point. Pass extra flags straight through, e.g.:
REM     ingest_all_stats.bat --sensors HU GMM --stats median
python ingest\ingest_all_stats.py %*
