# Ka대역 영상 자료 전송

수신 완료된 보관 영상만 전송합니다. 수신기와 감시기와 별도로 실행합니다. PostgreSQL 모드에서는 기존 `tb_ka_data`와 연결된 상세 기록을 조회하고 전송 보조 테이블에 이력을 저장합니다. [POSTGRESQL.md](POSTGRESQL.md)의 설정을 먼저 완료하세요. 아래 SQLite 파일 설명은 로컬 시험 모드 기준입니다. 실제 목적지 연동 규격이 미정이므로 현재 방식은 자체 HTTP 파일 업로드 시험 규격입니다.

## 로컬 시험

먼저 [수신기 실행 방법](KA_RECEIVER.md)에 따라 영상과 이름이 같은 `.dump`를 넣고 `RECEIVED`를 확인하세요. 기존 수신 완료 자료가 있으면 이를 사용할 수 있습니다.

PowerShell 창 하나에서 영상을 받는 로컬 시험 서버를 실행합니다.

```powershell
cd D:\Project\C26HDCS_Python
& .\.venv\Scripts\python.exe -m ka_receiver.test_sink
```

다른 PowerShell 창에서 전송 모듈을 실행합니다.

```powershell
cd D:\Project\C26HDCS_Python
& .\.venv\Scripts\python.exe -m ka_receiver.sender --destination-id local-ka --url http://127.0.0.1:8766/ka --camera-id cam_01 --once
```

출력의 `sent`가 1 이상이면 대상 서버의 저장 확인까지 완료된 것입니다. 받은 영상은 `data\ka_test_received\전송ID\원본파일명`으로 저장됩니다. 파일명과 확장자 대소문자는 변경하지 않으며, 같은 이름의 다른 자료는 전송 ID 폴더로 구분합니다. 메타데이터는 `uploads.sqlite3`에 남습니다. 같은 명령을 다시 실행해도 이미 전송 완료한 자료는 보내지 않습니다. 새 자료를 계속 자동 전송하려면 `--once`를 빼세요. 각 프로그램은 `Ctrl+C`로 종료합니다.

수정 전에 저장된 자료의 경로는 변경하지 않습니다. 기존 전송 ID의 재시도는 기존 저장 경로를 유지하며, 새 전송부터 원본 파일명 보관 방식을 적용합니다. HTTP 전송 헤더 `X-Ka-Filename`은 원본 파일명을 UTF-8 URL 인코딩하여 전달합니다. 외부 수신 서버도 이 값을 디코딩해 파일명으로 사용해야 합니다.

`--camera-id`를 생략하면 모든 카메라의 수신 완료 영상을 대상으로 합니다. 최초 실행 시 기존 수신 완료 자료도 포함합니다. 기본 DB는 수신기와 같은 `data\ka_receipts.sqlite3`입니다. 수신기에 다른 DB를 지정했다면 전송기의 `--db`도 동일하게 지정해야 합니다.

## 처리 흐름과 이력

대상별로 영상 수신 기록을 전송 대기열에 등록하고 `PENDING → SENDING → SENT` 순서로 처리합니다. 파일 누락·손상·통신 오류·잘못된 저장 확인 응답이면 `RETRY`로 기록하고 기본 30초부터 최대 900초까지 재시도 간격을 늘립니다. 전송 시도 이력은 `ka_delivery_attempts`, 현재 상태는 `ka_delivery_jobs`에 남습니다. 기록 시각은 UTC입니다.

전송 전에 수신 당시 크기와 SHA-256을 확인하고 1 MiB 단위로 읽어 전송합니다. 파일 전체를 메모리에 올리지 않습니다. 전송 시도 중에는 처리 권한의 유효 시간을 갱신합니다. 비정상 종료로 `SENDING`이 남으면 유효 시간 만료 후 다시 시도합니다. 기본 유효 시간은 90초이며 `--timeout-seconds`에 따라 달라집니다. 원본 영상·보관 영상·수신용 `.dump`는 삭제하지 않습니다.

재시도는 같은 `delivery_id`로 파일 처음부터 다시 전송합니다. 이어 보내기는 구현하지 않았습니다. 응답이 유실되면 재전송될 수 있으므로 실제 목적지도 전송 ID를 기준으로 중복 처리를 막아야 합니다. 로컬 시험 서버는 이 규칙을 구현하며 재시작해도 중복 처리 기록을 유지합니다.

`--destination-id`는 목적지의 고정 이름이며 최초 URL에 연결됩니다. 다른 URL로 보내려면 새 목적지 이름을 사용하세요. 새 목적지에는 해당 카메라의 기존 수신 완료 영상도 다시 전송됩니다. 실행 옵션은 `--help`로 확인할 수 있습니다. `--once`는 최대 10개를 처리한 뒤 종료하며 `--max-jobs`로 조정합니다. 한 번의 대기열 등록은 최대 500개이며 계속 실행하면 나머지도 차례로 등록됩니다.

## HTTP 시험 규격

`transport.py`가 전송 방식을 담당하고 `sender.py`가 대기열을 관리합니다. 실제 연동 규격이 정해지면 전송 부분을 맞춰 변경할 수 있습니다.

- POST 본문: 영상 원본 바이트 (`application/octet-stream`, `Content-Length` 지정)
- `Idempotency-Key`: 고정 전송 ID
- `X-Content-SHA256`: 영상 해시
- `X-Ka-Camera-Id`, `X-Ka-Receipt-Id`, `X-Ka-Filename`, `X-Ka-Received-At`: 수신 자료 메타데이터. 카메라 ID와 파일명은 URL 인코딩
- 성공 응답: HTTP 2xx와 아래 JSON. 전송 ID·크기·해시가 원본과 일치해야 완료 처리

```json
{"stored": true, "delivery_id": "전송 ID", "byte_size": 12345, "sha256": "영상 SHA-256"}
```

원래 `.dump`의 내용은 현재 알 수 없어 전달하지 않습니다. 목적지에는 영상과 메타데이터를 보내고 위 저장 확인 응답으로 완료를 판단합니다. HTTP 리다이렉트는 따라가지 않습니다. 로컬 서버는 `127.0.0.1`에서만 수신하며, 파일 하나당 기본 최대 10 GiB를 허용합니다. 인증과 실제 VMS 연동은 운영 시스템 규격이 정해진 뒤 추가해야 합니다.
