# Ka대역 CCTV 영상 파일 수신

수신기 동작 및 영상 도착 상태 감시는 [KA_MONITOR.md](KA_MONITOR.md)를 참고하세요.

수신 완료 영상의 HTTP 시험 전송은 [KA_SENDER.md](KA_SENDER.md)를 참고하세요.

여기서 Ka대역 수신은 지상 수신 시스템이 전달한 **영상 파일을 적재하는 단계**입니다. Python이 위성 전파를 직접 복조하지는 않습니다. 실제 장비의 전달 경로와 영상 형식은 아직 미확인입니다. PostgreSQL 연결 설정은 [POSTGRESQL.md](POSTGRESQL.md)를 참고하세요. FFmpeg는 필요하지 않으며 DB 연결 패키지는 `requirements.txt`로 설치합니다. 아래 SQLite 파일 경로 설명은 `HDCS_DB_BACKEND=sqlite`인 경우에 해당합니다.

## 수신 완료 조건

현재는 `clip001.mp4`와 `clip001.dump`처럼 **확장자를 뺀 이름이 같은 두 파일**을 한 쌍으로 가정합니다. `.dump`의 내용은 해석하지 않습니다. 영상과 `.dump`가 모두 생긴 뒤, 두 파일의 수정 시각이 최소 2초 동안 변하지 않고 영상 크기가 0보다 클 때 보관합니다. 파일을 복사하는 동안 크기나 수정 시각이 바뀌면 이번 시도를 버리고 다음 순회에서 다시 확인합니다. 같은 이름의 영상이 확장자만 다르게 둘 이상 있으면 자동 선택하지 않고 오류를 출력합니다.

**중요:** 2초 동안 변화가 없는 것은 완전 수신을 보장하지 않습니다. 실제 운영에서는 송신 측이 영상 및 `.dump` 전송을 완전히 끝낸 후 완료 신호를 원자적으로 게시하거나, 별도의 파일 크기·해시 정보가 필요합니다. 이 계약은 장비 규격이 확인되면 맞춰야 합니다.

## PowerShell 실행

```powershell
cd D:\Project\C26HDCS_Python
New-Item -ItemType Directory -Force .\data\ka_incoming | Out-Null
& .\.venv\Scripts\python.exe -m ka_receiver --watch-dir .\data\ka_incoming --camera-id cam_01
```

다른 PowerShell 창에서 시험 영상 `clip001.mp4`를 `data\ka_incoming`에 복사합니다. 복사가 끝난 뒤 완료 신호를 만듭니다.

```powershell
New-Item -ItemType File .\data\ka_incoming\clip001.dump | Out-Null
```

수신기 출력에 `RECEIVED`가 나타나면 적재된 것입니다. 종료는 `Ctrl+C`입니다. 폴더를 한 번만 확인할 때는 `--once`, 파일이 이미 완전히 써진 통제된 로컬 시험에서만 `--settle-seconds 0`을 사용합니다.

보관 영상은 `data\ka_video\cam_01\수신ID\원본파일명`에 들어갑니다. 파일명과 확장자 대소문자는 변경하지 않으며, 같은 이름의 다른 자료도 별도 수신 ID 폴더에 보관하여 덮어쓰지 않습니다. `data\ka_receipts.sqlite3`의 `ka_video_receipts` 테이블에는 카메라 ID, 원본·보관 경로, 크기, SHA-256, UTC 수신 시각을 남깁니다. PostgreSQL 사용 시 기존처럼 `tb_ka_data.file_name`에 원본 파일명을, `file_path`에 실제 보관 경로를 저장합니다. 원본 영상과 `.dump`는 삭제하지 않습니다. 같은 완료 신호를 다시 보거나 수신기를 재시작해도 중복 적재하지 않습니다. 같은 이름으로 새 영상을 받을 때는 영상을 교체하고 `.dump`의 수정 시각이 달라지도록 새로 게시해야 합니다.

기존에 임의 ID 파일명으로 보관된 영상은 자동으로 이름을 변경하지 않습니다. 새로 수신하는 영상부터 원본 파일명 보관 방식을 적용합니다.

기본 지원 확장자는 `.mp4`, `.mkv`, `.ts`, `.avi`, `.mov`, `.dav`입니다. 장비가 다른 확장자를 사용하면 `--extensions .mp4,.bin`처럼 지정합니다. 영상의 실제 재생 가능 여부나 코덱은 아직 검사하지 않습니다. `.dump`의 실제 의미, 파일명 규칙, 영상 형식, 카메라별 폴더 구조는 실데이터로 확인해야 합니다. Ka 시스템 감시와 영상 전송은 별도 단계입니다.
