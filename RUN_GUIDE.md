# 실제 PostgreSQL DB를 사용하는 실행 순서

이 가이드는 실제 DB에 수신 자료·감시·전송 기록을 남기는 시험입니다. 전송 목적지만 PC 내부의 HTTP 시험 서버를 사용합니다. 웹 서버는 Python 실행에 필수는 아니며 Python이 웹의 설정 파일을 읽어 PostgreSQL에 직접 접속합니다.

현재 Ka 매핑은 `cam_test → device_id 86`, 그룹은 2입니다. DCS 매핑은 아직 없습니다. 시험 장비 IP `127.0.0.1`은 로컬 시험용이며 실제 위성 장비에 연결된 주소가 아닙니다.

## 1. 모든 PowerShell 창의 공통 준비

새 PowerShell 창을 열 때마다 아래 두 줄을 실행하세요. 가상환경 활성화 없이 해당 환경의 Python을 직접 실행합니다.

```powershell
cd D:\Project\C26HDCS_Python
$env:HDCS_DB_BACKEND='postgresql'
```

연결 확인은 한 창에서 한 번 실행하면 됩니다.

```powershell
& .\.venv\Scripts\python.exe -m hdcs_db check
```

`connected: true`, `schema_ready: true`, `missing: []`이면 준비된 상태입니다. 드라이버는 설치되어 있습니다. 다른 PC에서 준비한다면 `python -m pip install -r requirements.txt`를 먼저 실행하세요. 현재 `.env`는 웹 프로젝트의 `application.yml`에서 접속 정보를 읽도록 설정돼 있습니다.

## 2. Ka 수신·감시·전송 시험

먼저 Ka만 실행해 확인하세요. 창 1~4의 프로그램은 계속 실행되므로 각각 다른 PowerShell 창에서 실행합니다. 모든 창에서 위 공통 준비 두 줄을 먼저 실행하세요.

### 창 1: 영상 수신기

```powershell
New-Item -ItemType Directory -Force .\data\ka_trial | Out-Null
& .\.venv\Scripts\python.exe -m ka_receiver --watch-dir .\data\ka_trial --camera-id cam_test
```

`Watching ... for camera cam_test`가 나오면 입력 폴더를 확인하고 있는 상태입니다. 영상은 `data\ka_video\cam_test`에 보관되고, 자료 메타데이터는 실제 DB의 `tb_ka_data`와 `tb_ka_receive_details`에 저장됩니다.

### 창 2: Ka 동작 상태 감시

```powershell
& .\.venv\Scripts\python.exe -m ka_receiver.monitor --camera-id cam_test
```

처음과 상태 변경 때만 출력하므로 화면에 계속 글자가 나오지 않아도 정상입니다. 영상이 아직 없으면 `RUNNING / NO_DATA / NONE`일 수 있습니다.

### 창 3: 영상을 받을 로컬 시험 서버

```powershell
& .\.venv\Scripts\python.exe -m ka_receiver.test_sink
```

주소는 `http://127.0.0.1:8766/ka`입니다. 이 프로그램이 전송 목적지 역할을 합니다. 받은 시험 영상은 `data\ka_test_received`에 저장합니다.

### 창 4: Ka 영상 전송기

```powershell
& .\.venv\Scripts\python.exe -m ka_receiver.sender --destination-id local-ka --url http://127.0.0.1:8766/ka --camera-id cam_test
```

새 수신 완료 영상을 찾아 자동으로 전송합니다. 이 목적지로 아직 보내지 않은 기존 `cam_test` 자료도 대상입니다. 목적지 ID는 계속 같은 값을 사용하세요. 다른 이름을 사용하면 별도 목적지로 간주해 기존 자료도 다시 보냅니다.

### 창 5: 시험 영상 넣기

탐색기에서 작은 실제 MP4 영상을 `D:\Project\C26HDCS_Python\data\ka_trial`에 복사하고 이름을 `test001.mp4`로 지정하세요. 이미 그 이름으로 시험했다면 `test002.mp4`처럼 새로운 이름을 쓰세요. **영상 복사가 끝난 뒤** 같은 이름의 완료 신호를 만듭니다.

```powershell
New-Item -ItemType File .\data\ka_trial\test001.dump
```

`test001.mp4.dump`가 아니라 `test001.dump`입니다. 기본적으로 파일 수정이 2초 이상 멈춘 뒤 적재합니다. 완료 신호는 시험자가 만드는 것이며, 실제 장비에서는 파일 전달 규칙에 맞춰 생성되어야 합니다.

확인할 결과는 다음과 같습니다.

| 위치 | 정상 결과 |
| --- | --- |
| 창 1 수신기 | `RECEIVED` |
| 창 2 감시기 | receiver_status=`RUNNING`, data_status=`RECEIVING`, pending_status=`NONE` |
| 창 3 시험 서버 | `stored: true` |
| 창 4 전송기 | `sent`가 1 이상인 결과 |
| 시험 서버 보관 폴더 | 전송 ID를 이름으로 한 영상 파일 생성 |

완료 신호가 없으면 영상은 보관 완료 처리되지 않고 `WAITING` 상태가 됩니다. 영상 재생 가능 여부는 수신 프로그램이 검사하지 않으므로 실제 영상으로 시험하세요.

## 3. Ka DB 기록 확인

DB 편집기에서 실행하세요.

```sql
SELECT ka_data_id, device_id, file_name, file_path, file_size,
       receive_status, received_at
FROM public.tb_ka_data
WHERE device_id = 86
ORDER BY ka_data_id DESC LIMIT 10;

SELECT * FROM public.tb_ka_monitor_status WHERE camera_id = 'cam_test';

SELECT j.id, j.receipt_id, j.destination_id, j.status, j.attempts,
       j.last_error, j.sent_at
FROM public.tb_ka_delivery_jobs j
JOIN public.tb_ka_data k ON k.ka_data_id = j.receipt_id
WHERE k.device_id = 86 AND j.destination_id = 'local-ka'
ORDER BY j.id DESC LIMIT 10;

SELECT a.* FROM public.tb_ka_delivery_attempts a
JOIN public.tb_ka_delivery_jobs j ON j.id = a.job_id
WHERE j.destination_id = 'local-ka'
ORDER BY a.id DESC LIMIT 10;
```

수신 상태 `SUCCESS`와 전송 상태 `SENT`를 확인하세요. 감시 JSON의 시간과 보조 이력은 시간대를 포함하며, 기존 `tb_ka_data.received_at`은 한국 시각으로 저장합니다. 기존 `tb_device.last_received_at`이나 `tb_device_status`를 갱신하는 기능은 아직 연결하지 않았으므로 이 시험에서는 위 자료·보조 테이블을 확인하세요.

## 4. L대역 DCS 시험 준비

DCS는 사용할 실제 장비 ID를 먼저 선택해야 합니다. DB에서 확인합니다.

```sql
SELECT device_id, device_name, device_type, group_id
FROM public.tb_device
ORDER BY device_id;
```

PDF 샘플 코드는 `44/56/01`입니다. 시험 대상으로 정한 L대역 장비 ID를 아래 명령의 입력 요청에 넣으세요. 존재하지 않는 ID는 등록되지 않습니다.

```powershell
& .\.venv\Scripts\python.exe -m hdcs_db map-device --kind DCS --external-key '44/56/01' --device-id (Read-Host 'DCS 시험 장비의 실제 device_id')
```

Ka 장비 86을 DCS 시험 장비로 사용하지 마세요. DCS 매핑은 한 번 등록하면 유지됩니다.

## 5. L대역 수신·감시·전송 실행

Ka 시험을 마치고 창들을 재사용해도 됩니다. 각 창에서 공통 준비 후 다음 순서로 실행하세요.

### 창 1: TCP 수신기

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver tcp-listen
```

`127.0.0.1:9000`에서 시험 송신을 기다립니다.

### 창 2: DCS 상태 감시

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver monitor
```

### 창 3: DCS 자료를 받을 로컬 시험 서버

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver test-sink
```

주소는 `http://127.0.0.1:8765/dcs`입니다.

### 창 4: DCS 자료 전송기

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver send --destination-id local-dcs --url http://127.0.0.1:8765/dcs
```

매핑·해석이 정상인 `tcp:` 수신 자료 중 이 목적지에 아직 보내지 않은 자료가 대상입니다. 기존 대상 자료도 전송될 수 있습니다.

### 창 5: PDF 샘플을 TCP로 보내기

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver sample-tcp
```

창 1에서 `PARSED`, 창 2에서 `RECEIVING`, 창 3에서 JSON 자료, 창 4에서 `sent` 1 이상을 확인합니다. `UNMAPPED`이면 코드와 장비 ID 매핑을 확인하세요. `document_xor_match=False`는 제공 PDF 샘플에 이미 확인된 XOR 불일치이며 별도 기록합니다. 현재 정상 해석이 실제 장비 오류검출 검증 완료를 뜻하지는 않습니다.

## 6. DCS DB 기록 확인

```sql
SELECT b.dcs_data_id, b.device_id, b.data_payload, b.received_status,
       b.received_at, d.id AS detail_id, d.parse_status, d.mapping_error
FROM public.tb_dcs_data b
JOIN public.tb_dcs_receive_details d ON d.dcs_data_id = b.dcs_data_id
WHERE d.source LIKE 'tcp:%'
ORDER BY b.dcs_data_id DESC LIMIT 10;

SELECT * FROM public.tb_dcs_monitor_status WHERE source_filter = 'tcp:';

SELECT j.id, d.dcs_data_id, j.destination_id, j.status, j.attempts,
       j.last_error, j.sent_at
FROM public.tb_dcs_delivery_jobs j
JOIN public.tb_dcs_receive_details d ON d.id = j.frame_id
WHERE j.destination_id = 'local-dcs'
ORDER BY j.id DESC LIMIT 10;
```

`sample-tcp`를 실행할 때마다 새로운 실제 DB 시험 이력이 생깁니다. 기록은 자동 삭제하지 않습니다.

## 7. 종료와 문제 확인

시험 자료 입력을 멈춘 뒤 수신기 창에서 `Ctrl+C`를 누르세요. 전송이 끝나면 전송기를 종료하고, 마지막으로 시험 서버와 감시기를 종료합니다. 수신기 종료 직후 Ka 감시에서 `STOPPED`로 바뀌는 것은 정상입니다.

- `schema_ready: false`: SQL 적용 대상 DB가 맞는지 확인합니다.
- `UNMAPPED` 또는 `No enabled KA device mapping`: 코드·카메라 ID 매핑을 확인합니다.
- Ka `WAITING`: 영상과 dump의 이름, 복사 완료 여부를 확인합니다.
- 전송 `retry_scheduled`: 시험 서버를 먼저 실행했는지 확인합니다. 기본 재시도는 30초부터 최대 900초 간격입니다.
- `Address already in use`: 같은 수신기 또는 시험 서버가 이미 실행 중인지 확인합니다. 이 가이드의 프로그램을 중복 실행하지 마세요.
- 전송 `sent: 0`: 아직 수신 자료가 없거나, 이미 전송했거나, 재시도 시각이 되지 않은 상태일 수 있습니다.
- 감시 `STALE`: 기본 600초 동안 새 자료를 받지 않았다는 뜻입니다. 다음 시험 자료가 정상 도착하면 회복됩니다.

전체 테스트를 한 번에 진행하기 어렵다면 먼저 Ka 수신기만 켜서 `tb_ka_data`에 기록이 생기는 것을 확인하고, 감시기와 전송기를 차례로 추가하세요.
