# Ka대역 영상 수신 동작 상태 감시

PostgreSQL 모드에서는 같은 명령이 운영 DB의 보조 테이블에 기록하며 `--db` 경로는 사용하지 않습니다. 먼저 [POSTGRESQL.md](POSTGRESQL.md)의 SQL 적용·매핑 설정을 완료하세요. 아래 SQLite 관련 설명은 로컬 시험 모드 기준입니다.

카메라별로 Python 수신기의 동작 기록, 영상 수신 완료 기록, 완료되지 않은 파일 쌍을 확인합니다. 현재 감시 대상은 파일 수신 프로그램입니다. 위성 모뎀의 신호 세기나 CCTV 장비 자체의 상태를 확인하려면 장비 상태 인터페이스가 추가로 필요합니다.

## 실행

기존 수신기가 실행 중이면 업데이트된 코드로 다시 실행하세요. 수신기는 아래처럼 실행합니다.

```powershell
cd D:\Project\C26HDCS_Python
New-Item -ItemType Directory -Force .\data\ka_incoming | Out-Null
& .\.venv\Scripts\python.exe -m ka_receiver --watch-dir .\data\ka_incoming --camera-id cam_01
```

별도 PowerShell 창에서 감시 프로그램을 실행합니다.

```powershell
cd D:\Project\C26HDCS_Python
& .\.venv\Scripts\python.exe -m ka_receiver.monitor --camera-id cam_01
```

처음 한 번과 상태 변경 시 JSON 결과를 출력합니다. 한 번만 조회하려면 `--once`를 붙입니다. 종료는 `Ctrl+C`입니다. 두 프로그램의 `--camera-id`와 `--db`는 같아야 합니다. 기본 DB는 프로젝트의 `data\ka_receipts.sqlite3`입니다. 카메라별로 고정된 입력 폴더를 사용하고, 같은 카메라 ID의 수신기는 하나만 실행하세요.

## 결과 해석

세 항목을 함께 보면 수신기 동작과 영상 도착을 구분할 수 있습니다.

| 항목 | 상태 | 의미 |
| --- | --- | --- |
| receiver_status | NEVER_STARTED | 해당 카메라의 수신기 동작 기록 없음 |
| receiver_status | RUNNING | 수신기가 주기적으로 동작 기록을 갱신함 |
| receiver_status | STOPPED | 수신기가 정상 종료됨 (`--once` 종료 포함) |
| receiver_status | UNRESPONSIVE | 동작 기록이 기준 시간 동안 갱신되지 않음. 비정상 종료 또는 처리 정체 가능 |
| receiver_status | ERROR | 최근 폴더 확인이나 파일 처리에서 오류 발생 |
| data_status | NO_DATA | 해당 카메라의 영상 수신 완료 이력 없음 |
| data_status | RECEIVING | 기준 시간 안에 영상 수신 완료 기록이 있음 |
| data_status | STALE | 마지막 영상 수신 완료 이후 기준 시간 초과 |
| pending_status | NONE | 최근 폴더 확인에서 완료 대기 자료 없음 |
| pending_status | WAITING | 영상 또는 `.dump` 누락, 빈 영상, 파일 안정화 등으로 대기 중 |
| pending_status | DELAYED | 완료 대기 자료가 기준 시간 이상 남아 있음 |

예를 들어 `RUNNING / NO_DATA / DELAYED`는 수신기는 작동하지만 영상 수신이 완료되지 않은 채 오래 대기 중이라는 뜻입니다. `STOPPED / RECEIVING`은 최근 영상은 받았지만 현재 수신기는 종료됐다는 뜻입니다. 대기 정보와 오류 정보는 마지막 수신기 확인 결과이므로 `UNRESPONSIVE`나 `STOPPED`일 때 현재 폴더 상태로 해석하지 마세요.

기본값은 동작 기록 중단 30초, 완료 영상 미수신 600초, 완료 대기 300초, 감시 확인 간격 5초입니다. 실제 카메라의 영상 전송 주기와 파일 크기에 맞춰 변경하세요.

```powershell
& .\.venv\Scripts\python.exe -m ka_receiver.monitor --camera-id cam_01 --heartbeat-timeout-seconds 30 --stale-after-seconds 600 --pending-after-seconds 300 --interval-seconds 5
```

수신기의 `--poll-seconds`를 늘렸다면 동작 기록 중단 기준은 그 간격보다 충분히 크게 설정하세요. 큰 파일을 복사하는 동안에도 동작 기록을 갱신합니다. 완료 대기 시간은 수신기가 처음 대기 자료를 발견한 시각부터 계산하며 재시작해도 보존합니다. 영상과 `.dump` 중 누락된 쪽이 도착해 대기 항목이 바뀌면 새 항목의 대기 시간이 시작될 수 있습니다.

## 시험 방법

1. 빈 입력 폴더로 수신기를 실행하면 `RUNNING / NO_DATA / NONE`을 확인할 수 있습니다. 기존 수신 이력이 있으면 `data_status`는 그 이력을 기준으로 표시됩니다.
2. 영상만 넣고 `.dump`는 넣지 않으면 `pending_status`가 `WAITING`이 됩니다. 짧게 시험하려면 감시 명령에 `--pending-after-seconds 10`을 추가하고 10초 넘게 기다려 `DELAYED`로 바뀌는지 확인합니다.
3. 영상과 이름이 같은 `.dump`를 넣으면 수신 완료 후 `RECEIVING / NONE`이 됩니다.
4. 수신기 창에서 `Ctrl+C`로 종료하면 `receiver_status`가 `STOPPED`가 됩니다. 비정상 종료로 종료 기록을 남기지 못하면 동작 기록 중단 기준을 넘긴 뒤 `UNRESPONSIVE`가 됩니다.

시험은 파일 수신과 상태 판정을 검증합니다. 영상의 재생 가능 여부나 실제 위성 링크 상태를 검증하지는 않습니다.

## 기록

같은 SQLite DB의 `ka_receiver_runtime`에 카메라별 동작 기록과 최근 오류, `ka_receiver_pending`에 완료 대기 목록을 저장합니다. `ka_monitor_status`에는 최신 감시 결과를, `ka_monitor_events`에는 상태 또는 오류 변경 이력을 저장합니다. 시간은 UTC입니다. 감시 프로그램은 입력 파일을 이동하거나 삭제하지 않습니다.
