# L대역 DCS 샘플 수신기

제공된 `VHF_4량 데이터 프로토콜.pdf`의 35바이트 프레임을 분리하고 해석합니다. 현재 PostgreSQL 연동이 추가되어 있으며, `.env`에서 웹의 DB 설정을 읽도록 구성했습니다. 최초 설정과 장비 연결은 [POSTGRESQL.md](POSTGRESQL.md)를 먼저 확인하세요. 필요한 패키지는 `requirements.txt`에 있습니다.

아래 기존 로컬 시험 설명은 SQLite 모드 기준입니다. 시험 데이터를 운영 DB에 넣지 않고 따라 하려면 PowerShell에서 먼저 `$env:HDCS_DB_BACKEND='sqlite'`를 설정하세요. PostgreSQL 모드에서 `--db` 파일 경로는 사용하지 않습니다. 일반 자동 테스트는 스스로 SQLite 임시 DB를 선택합니다.

## 실행 (PowerShell)

Ka-band CCTV file receiving is documented in [KA_RECEIVER.md](KA_RECEIVER.md).

```powershell
cd D:\Project\C26HDCS_Python
& .\.venv\Scripts\python.exe -m dcs_receiver sample
& .\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

`sample`은 PDF의 예시를 한 번 수신한 것처럼 처리합니다. 결과는 `data/dcs_frames.sqlite3`의 `dcs_frames` 테이블에 기록됩니다. 다시 실행하면 새 수신 기록이 추가됩니다.

## 전체 흐름 시험

PowerShell 창을 3개 열고 아래 순서대로 실행합니다. 모두 같은 PC의 `127.0.0.1`만 사용하며 외부 시스템으로 전송하지 않습니다. 예제는 기존 DB와 구분하려고 `data/trial.sqlite3`을 사용합니다.

**창 1 - DCS 수신기**

```powershell
cd D:\Project\C26HDCS_Python
& .\.venv\Scripts\python.exe -m dcs_receiver --db .\data\trial.sqlite3 tcp-listen
```

**창 2 - 전송 자료를 받는 로컬 시험 서버**

```powershell
cd D:\Project\C26HDCS_Python
& .\.venv\Scripts\python.exe -m dcs_receiver test-sink
```

**창 3 - PDF 샘플 전송**

```powershell
cd D:\Project\C26HDCS_Python
& .\.venv\Scripts\python.exe -m dcs_receiver sample-tcp
```

창 1에 `PARSED`가 표시된 다음, 창 3에서 이어서 실행합니다.

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver --db .\data\trial.sqlite3 monitor --once --stale-after-seconds 600
& .\.venv\Scripts\python.exe -m dcs_receiver --db .\data\trial.sqlite3 send --destination-id local-demo --url http://127.0.0.1:8765/dcs --once
```

창 1에 `PARSED`, 창 3의 감시 결과에 `RECEIVING`, 전송 결과에 `"sent": 1` 이상, 창 2에 JSON 자료가 보이면 전체 경로가 동작한 것입니다. 같은 시험을 다시 실행하면 새로 수신한 자료만 추가 전송됩니다. 마치려면 창 1과 2에서 `Ctrl+C`를 누릅니다.

실제 원시 바이트 파일이나 16진수 문자열을 시험하려면 다음처럼 실행합니다.

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver file .\capture.bin
& .\.venv\Scripts\python.exe -m dcs_receiver hex '02 30 31 34 34 35 36 31 35 31 32 39 30 30 34 34 33 35 30 46 30 30 30 36 30 30 34 34 35 30 30 30 30 56 0D'
```

TCP로 바이트를 보내는 장비 또는 시험 송신기를 연결할 때만 다음 명령을 사용합니다. 기본값은 로컬 PC에서만 접속 가능한 `127.0.0.1:9000`입니다.

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver tcp-listen --host 127.0.0.1 --port 9000
```

## L대역 수신 상태 감시

별도 PowerShell 창에서 감시 명령을 실행합니다. 아래의 600초는 임시 기준이며 실제 관측소 자료의 송신 주기에 맞춰 조정해야 합니다.

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver monitor --stale-after-seconds 600
```

처음 한 번과 상태가 바뀔 때마다 결과를 출력합니다. 한 번만 확인하려면 `--once`를 사용합니다.

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver monitor --once --stale-after-seconds 600
```

기본 감시는 `tcp:` 출처만 포함합니다. PDF 샘플이나 파일 재생은 운영 수신으로 계산하지 않습니다. 시험 자료까지 포함해 동작을 확인할 때만 `--all-sources`를 붙입니다. 실제 장비의 수신 방식이 TCP가 아니라면 `--source-prefix`를 해당 출처에 맞춰 변경해야 합니다.

- `NO_DATA`: 해당 출처의 수신 기록이 한 번도 없음
- `RECEIVING`: 설정 시간 안에 정상 파싱된 자료가 있음
- `INVALID_ONLY`: 최근 원본은 들어오지만 정상 파싱된 자료가 없음
- `STALE`: 설정 시간 동안 원본 자료가 들어오지 않음

현재 상태는 SQLite의 `dcs_monitor_status`, 상태 변경 이력은 `dcs_monitor_events`에 저장됩니다. 이 감시는 시스템 전체의 자료 도착 여부를 판단합니다. 관측소별 누락 감시에는 실제 국번·그룹과 241개 관측소를 연결한 목록 및 관측소별 송신 주기가 추가로 필요합니다.

시간은 DB와 명령 출력 모두 UTC로 표시됩니다. 여기서 `RECEIVING`은 프레임 형식이 파싱되었다는 뜻이며, 아직 확정되지 않은 XOR 오류검출 규칙까지 통과했다는 의미는 아닙니다.

## DCS 자료 전송

수신 시스템의 연동 규격이 아직 없어 현재 송신 방식은 **HTTP POST JSON 시험 구현**입니다. 실제 주소와 JSON 형식이 확정되기 전에는 운영 시스템에 연결하지 마세요. 수신기와 별도 PowerShell 창에서 다음과 같이 실행합니다.

```powershell
& .\.venv\Scripts\python.exe -m dcs_receiver send --destination-id target-a --url 'https://REPLACE-WITH-DESTINATION.example/api/dcs'
```

`--destination-id`는 전송 대상의 고정된 이름입니다. 대상을 바꾸면 새 이름을 사용해야 기존 전송 완료 기록과 섞이지 않습니다. 한 번만 전송 가능한 자료를 처리하려면 `--once`를 붙입니다. 기본적으로 한 번에 최대 100건을 처리하며 `--max-jobs`로 변경할 수 있습니다.

정상 파싱된 `tcp:` 수신 자료만 전송 대기열에 넣습니다. PDF 샘플·파일 재생 자료를 로컬 시험 서버로 보내볼 때만 `--all-sources`를 붙입니다. JSON에는 국번·그룹·전압·4량 값·원본 16진수·PDF 기준 XOR 비교 결과가 포함됩니다. 원래 값의 앞자리 0을 보존하도록 수치 필드는 문자열로 전송합니다.

전송 성공은 HTTP 2xx 응답으로 판단합니다. 실패하면 30초부터 최대 900초까지 간격을 늘려 계속 재시도합니다. `dcs_delivery_jobs`에는 현재 상태, `dcs_delivery_attempts`에는 시도 이력이 남습니다. 네트워크 응답을 받기 전에 프로세스가 종료되면 같은 `Idempotency-Key`로 다시 보낼 수 있으므로, 실제 수신 시스템도 이 값을 이용해 중복 처리를 방지해야 합니다.

현재 목적지 주소가 없으므로 외부 시스템으로 실제 전송하지 않았습니다. 로컬 HTTP 시험 서버를 사용하는 자동 테스트는 `python -m unittest discover -s tests -v`로 확인할 수 있습니다.

PDF 예시의 1~25바이트 XOR 계산값은 `0x78`인데 수신된 에러검출코드는 `0x56`입니다. 이 차이는 DB에 기록하며 파싱 성공 여부와 분리합니다. 실제 L대역 장비의 입력 방식, 에러검출 규칙, 값의 단위와 관측소 코드 매핑은 실데이터 또는 장비 사양으로 확인해야 합니다. 현재 TCP 입력은 시험용 연결 방식이며 위성 장비의 실제 인터페이스가 확정된 것은 아닙니다.
