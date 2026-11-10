# Python Device Logs and Web Alarms

## Deployment

1. In the existing WebService PostgreSQL database, execute
   `D:/Project/C26HDCS_WebService/src/main/resources/sql/python_telemetry_postgresql.sql`.
   It creates only `tb_python_alarm_state`, used to suppress repeated fault alarms.
   Existing pipeline tables, `tb_log` and `tb_alarm` must already exist.
   The application DB account needs INSERT on logs/alarms, sequence USAGE,
   and SELECT/INSERT/UPDATE on the incident state table.
2. Keep the existing PostgreSQL connection settings and enabled device mappings.
   Add these settings to the Python project's existing `.env` for receiver/system
   failures that cannot identify a station/camera:

   ```dotenv
   HDCS_DB_BACKEND=postgresql
   HDCS_DCS_DEVICE_ID=75
   # Use the actual registered Ka device ID:
   HDCS_KA_DEVICE_ID=86
   HDCS_LOG_DIR=D:/Project/C26HDCS_Python/logs
   ```

   These fallback IDs identify the monitored receiver/system; they do not create
   devices. Normal identified data uses its enabled mapping first. With no key,
   a single unambiguous mapping can be used; multiple mappings require an explicit
   fallback ID. Never assign all stations to one device simply to hide mapping errors.
3. Restart the Python receivers, monitors and senders, and restart WebService.
   Existing receive/send commands remain valid. For one DCS station use:

   ```powershell
   cd D:\Project\C26HDCS_Python
   & .\.venv\Scripts\python.exe -m hdcs_db check
   & .\.venv\Scripts\python.exe -m dcs_receiver monitor --station-key 44/56/01 --interval-seconds 1
   # In another window:
   & .\.venv\Scripts\python.exe -m ka_receiver.monitor --camera-id cam_test --interval-seconds 1
   ```

   Run a station-scoped DCS monitor for each expected station. A global receiver
   monitor cannot detect a silent station while another station keeps sending.
   Monitor thresholds remain configurable; the one-second check interval is NOT
   a promise that all normal event logs are generated every second.

## Records

- `hdcs_telemetry.py` is the shared Python module. PostgreSQL operations commit
  device logs/alarms together with receive, monitoring or delivery-result state.
- `tb_log`: successful receptions, monitor state transitions, delivery attempt
  outcomes, receiver start/stop, fault and recovery records. Existing pipeline
  detail/event/attempt tables continue to be written.
- `tb_alarm`: invalid/unmapped DCS data (associated receiver required), Ka file
  access/processing errors, receive silence, delayed completion markers, missing
  receiver heartbeat, and transmission failures. Alarm categories match the web
  filters (system/network/data); severities are HIGH or MEDIUM.
- Fault state is keyed by device + component + scope/destination, not TCP source
  ephemeral ports. Repeated detection/retries do not add duplicate incident alarms.
  Recovery adds an INFO record and rearms the incident for a future recurrence.
  Historical alarms remain; recovery does not mark them read on behalf of users.
- `tb_event` is reserved for actual authenticated user actions and has a user FK.
  Automatic Python device operations use `tb_log`, not a fabricated SYSTEM user.
- When a device cannot be identified, only the file log is written; no fake device
  association is created. Configure the fallback IDs before operating unattended.

## Web Display and Timing

The web server checks committed `tb_alarm` changes once per second and pushes
unread counts over the existing authenticated SSE stream. The alarm list and
an open header preview refresh when the SSE event arrives. Detection time also
depends on the Python check interval, configured timeout, DB/network latency and
browser connection. This is not a hard real-time guarantee or a new sound/popup.
Browsers without streaming retain the existing 15-second fallback count polling.

## Failure Boundaries and File Logs

Each Python process writes UTF-8 date-based files containing PID and a startup
UUID, with a one-second file heartbeat and fsync. Completed days are gzip
compressed and retained for at least five calendar years; there is no early
size/count-based purge. See `LOG_RETENTION.md` for the current policy and required
WebService DB migration. Messages are bounded, flattened to one line,
and do not include raw frames, videos or configured endpoint credentials.
Normal DB logs propagate persistence failures; they are not silently treated
as successfully committed. File entries describe attempts and can survive DB
transaction rollback.

Unhandled operational errors use a fresh DB connection for a best-effort system
alarm and preserve the original exception. If the DB itself is unavailable,
neither Python nor WebService can save/display a DB alarm; file/stderr remains
the fallback. A killed DCS receiver with no more data is detected as stale by its
separate monitor. A killed Ka receiver is detected by heartbeat timeout. A killed
monitor/sender cannot diagnose itself; use an external process supervisor for
process restart and out-of-band infrastructure alerts.

## Checks

```sql
SELECT * FROM public.tb_log WHERE device_id IN (75,86) ORDER BY reg_dt DESC, id DESC LIMIT 50;
SELECT * FROM public.tb_alarm WHERE device_id IN (75,86) ORDER BY occurred_at DESC, alarm_id DESC LIMIT 50;
SELECT * FROM public.tb_python_alarm_state WHERE device_id IN (75,86);
```

On test data only: cause a Ka ambiguous pair (`clip.mp4`, `clip.mkv`, `clip.dump`),
scan repeatedly, and verify one incident alarm. Remove the extra video and
confirm recovery. Recreate the fault and verify a new alarm. To test delivery,
stop the local HTTP test sink, send once, restart it and wait for retry. Never
deliberately interrupt a production endpoint to test alarms.

Run `python -m unittest discover -s tests -v` with the SQLite backend for regression
tests. Optional PostgreSQL telemetry tests accept only a disposable localhost
`hdcs_test_*` DB and require `HDCS_TEST_TELEMETRY_POSTGRES=1` and
`HDCS_TELEMETRY_SQL` pointing at the migration file.
