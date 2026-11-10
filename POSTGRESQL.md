# 기존 PostgreSQL DB 연동

수신·감시·전송 프로그램이 공통 연결 모듈 `hdcs_db.py`를 통해 PostgreSQL을 사용하도록 수정했습니다. `.env`는 Git 제외 파일이며 현재 웹 프로젝트의 `application.yml` 경로를 가리킵니다. 비밀번호를 Python 소스에 복사하지 않았습니다. 공통 YAML 문서의 `spring.datasource`를 읽으며 프로필 전용 datasource는 자동 선택하지 않습니다. 프로필별 접속이 필요하면 PG* 환경변수로 명시하세요.

## 적용 순서

1. 웹 프로젝트의 `src/main/resources/sql/python_pipeline_postgresql.sql` 전체를 대상 DB에서 실행합니다. 기존 `schema.sql`이 아닙니다. 기본 테이블을 삭제하거나 재생성하지 않습니다. PostgreSQL 모드의 Python은 테이블을 자동 생성하지 않습니다.
2. 기존 `tb_device.device_id`를 확인하고 DCS 코드 및 Ka 카메라 ID를 연결합니다.
3. 연결 상태를 확인한 뒤 기존 수신·감시·전송 명령을 실행합니다.

```powershell
cd D:\Project\C26HDCS_Python
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
& .\.venv\Scripts\python.exe -m hdcs_db check
```

`connected: true`, `schema_ready: true`이면 연결과 보조 테이블 준비가 확인된 것입니다. 접속 확인은 읽기 전용입니다. 초기 확인 시 실제 서버 접속은 성공했으며 보조 테이블은 아직 없었습니다. 이 작업에서는 실제 서버에 SQL이나 시험 데이터를 적용하지 않았습니다.

## 장비 매핑

아래의 ID는 예시입니다. **실제 장비 ID로 바꾼 뒤** 실행하세요. 명령은 매핑 테이블에만 저장하며 장비를 새로 만들지 않습니다. 존재하지 않는 ID는 외래키 오류로 거절됩니다.

```powershell
& .\.venv\Scripts\python.exe -m hdcs_db map-device --kind DCS --external-key '44/56/01' --device-id 101
& .\.venv\Scripts\python.exe -m hdcs_db map-device --kind KA --external-key cam_01 --device-id 201
```

DCS 키는 `system_group_number/group_number/station_number` 순서이며 앞자리 0을 보존합니다. 위 값은 PDF 샘플의 코드입니다. Ka 키는 수신기의 `--camera-id`와 같습니다. 같은 키로 명령을 다시 실행하면 장비 연결이 변경되므로 이미 사용 중인 매핑을 바꿀 때는 실제 장비를 확인하세요.

매핑이 없는 DCS는 `UNMAPPED`로 출력하고 `tb_dcs_receive_details`에 원본과 사유를 보존합니다. 장비를 식별할 수 없는 오류 프레임도 상세 기록에만 남깁니다. 기본 자료 테이블에는 넣지 않고 전송에서 제외합니다. 감시의 `INVALID_ONLY`에는 PostgreSQL 모드에서 미매핑 수신도 포함됩니다. 매핑을 나중에 추가해도 과거 미매핑 자료를 자동으로 연결하거나 전송하지 않습니다. 필요하면 보존 원본을 별도로 재처리해야 합니다.

Ka 매핑이 없으면 수신 오류를 기록하고 원본 영상과 dump를 유지합니다. 매핑 등록 후 다음 순회에서 다시 처리합니다. 장비 매핑은 임의로 등록하지 않았습니다.

## 저장 구조

| 처리 | 기록 위치 |
| --- | --- |
| 정상 DCS 수신 | 기존 tb_dcs_data + tb_dcs_receive_details |
| 정상 Ka 수신 | 기존 tb_ka_data + tb_ka_receive_details |
| DCS 상태 감시 | tb_dcs_monitor_status, tb_dcs_monitor_events |
| Ka 상태 감시 | tb_ka_receiver_runtime, tb_ka_receiver_pending, tb_ka_monitor_status, tb_ka_monitor_events |
| DCS 전송 | tb_dcs_delivery_cursors, tb_dcs_delivery_jobs, tb_dcs_delivery_attempts |
| Ka 전송 | tb_ka_delivery_destinations, tb_ka_delivery_jobs, tb_ka_delivery_attempts |

기본 자료와 상세 정보는 한 트랜잭션으로 저장합니다. 상세 저장이 실패하면 기본 자료도 롤백합니다. 정상 상태는 웹이 인식하는 `SUCCESS`입니다. DCS `data_payload`에는 원본 16진수 문자열을 넣고 수문 값은 상세 테이블에 보존합니다. Ka `file_path`는 보관 경로이며 `file_name`은 원래 영상 이름입니다. 영상 파일 자체는 기존처럼 디스크에 보관합니다.

기존 기본 테이블의 timezone 없는 `received_at`은 한국 시각으로 기록합니다. 보조 기록은 TIMESTAMPTZ이고 Python 조회·전송 시각은 UTC입니다. 다른 시간대를 쓰려면 `HDCS_LOCAL_TIMEZONE`과 `v_python_ka_receipts`의 시간대 정의를 함께 바꿔야 합니다.

기존 자료 조회 화면은 기존 테이블의 새 자료를 조회할 수 있습니다. 기존 장비 상태·이벤트·전송 이력 테이블에는 아직 기록하지 않고 전용 보조 테이블을 사용합니다. 기존 업무 자료나 SQLite 기록의 이관은 포함하지 않았습니다. 새 상세 기록이 있는 자료만 전송 대상에 들어갑니다.

## 실행 명령

```powershell
# 각 장기 실행 명령은 별도 창에서 실행
& .\.venv\Scripts\python.exe -m dcs_receiver tcp-listen
& .\.venv\Scripts\python.exe -m dcs_receiver monitor
& .\.venv\Scripts\python.exe -m ka_receiver --watch-dir .\data\ka_incoming --camera-id cam_01
& .\.venv\Scripts\python.exe -m ka_receiver.monitor --camera-id cam_01
```

전송 명령과 HTTP 시험 서버 실행법은 `README.md`, `KA_SENDER.md`를 참고하세요. PostgreSQL 모드에서는 모든 명령이 같은 접속 설정을 사용하며 기존 `--db` 옵션은 사용하지 않습니다. 시험 수신 서버 자체의 로컬 파일 기록은 운영 DB와 별도입니다.

PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD 환경변수는 웹 설정의 해당 값을 덮어씁니다. PGSSLMODE도 지원합니다. 환경변수는 `.env`보다 우선합니다. SQLite 로컬 시험으로 돌아갈 때는 현재 PowerShell 창에서 다음을 설정합니다.

```powershell
$env:HDCS_DB_BACKEND='sqlite'
```

## 검증

일반 자동 테스트는 운영 설정과 무관하게 임시 SQLite DB를 선택합니다.

```powershell
& .\.venv\Scripts\python.exe -B -m unittest discover -s tests -v
```

PostgreSQL 통합 테스트는 `HDCS_TEST_POSTGRES=1`과 PostgreSQL 모드를 명시해야 실행되며, 접속 대상이 localhost이고 DB명이 `hdcs_test_`로 시작해야 합니다. 별도 테스트 DB에 사용자가 제공한 기본 테이블 정의를 구성한 뒤 SQL 재실행, 실제 수신 저장, 감시, 로컬 HTTP 전송, 재시도, 미매핑, 롤백을 검증합니다. 실제 서버에는 테스트를 실행하지 마세요.
