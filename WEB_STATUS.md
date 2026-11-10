# Python 상태와 웹 표시 연동

상세 적용·시험 안내: `D:/Project/C26HDCS_WebService/docs/PYTHON_WEB_STATUS.md`

이번 코드로 실행하기 전에 운영 PostgreSQL에 다음 SQL을 적용해야 합니다.

`D:/Project/C26HDCS_WebService/src/main/resources/sql/python_device_status_postgresql.sql`

기존 수신·감시·전송 프로세스를 종료하고 SQL 적용 후 WebService와 Python을 재시작합니다. Python의 PostgreSQL 모드는 테이블을 자동 생성하지 않습니다. 기존 SQLite 시험 모드는 그대로 사용할 수 있습니다.

`hdcs_status.py`는 모듈별 상태와 대표 상태를 관리하고 실제 수신 시각을 기록합니다. 대표 상태는 ERROR > WARNING > NORMAL입니다. 수신 정상만으로 전송 오류를 덮어쓰지 않습니다.

감시/수신/전송 프로세스의 갱신 유효 시간은 `HDCS_STATUS_TTL_SECONDS` 기본 120초입니다. 장시간 작업·점검 간격보다 충분히 길게 지정합니다. 관측소별 DCS 감시와 카메라별 Ka 감시를 지속 실행해야 합니다. `--once` 종료 후 TTL이 지나면 중단 상태로 표시됩니다.

WebService는 5초 간격으로 갱신 중단을 감지하고 장비 상태 이력과 시간 초과 알람/복구 로그를 저장합니다. 웹 대시보드도 5초마다 갱신합니다. 운영 DB가 중단되면 웹은 상태 조회 실패로 표시하며, DB에 장애를 기록할 수 없는 한계는 외부 DB 감시로 보완해야 합니다.
