# 1초 로그 생성과 5년 보관

실행 중인 DCS/Ka 수신·감시·전송 프로세스는 업무 입력이 없어도 1초마다
HEARTBEAT를 파일에 기록하고 flush/fsync한다. DB 업무 로그·알람은 기존대로
업무 발생 시 기록하며, 하트비트 때문에 초당 DB 행을 추가하지 않는다.

- 파일: logs/hdcs-python-PID-시작UUID.YYYY-MM-DD.분할번호.log
- 날짜 변경: 이전 파일 gzip 압축, 압축 전후 내용 SHA256 확인 후 원본 정리
- 보관: 한국 시각의 달력 기준 최소 5년. 경계 날짜 전체를 보존한다.
- 용량 상한/백업 개수 때문에 5년 이내 로그를 삭제하지 않는다.
- 여유 공간 1GB 미만은 LOG_DISK_LOW로 경고한다. 조기 삭제로 해결하지 않는다.
- 프로세스별 파일을 분리하여 동시 회전과 PID 재사용에 따른 덮어쓰기를 방지한다.
- 강제 종료로 남은 비압축 로그와 과거 형식의 로그는 임의 삭제하지 않는다.
- 환경변수 HDCS_LOG_DIR로 경로를 바꿀 수 있다. 웹 저장장치 화면의 보호 경로도
  HDCS_PYTHON_LOG_DIR에 같은 실제 경로로 설정해야 한다.

수신/감시/전송 프로세스는 변경 후 모두 재시작해야 새 정책을 사용한다.
운영 DB의 5년 보호는 WebService의
src/main/resources/sql/log_audit_retention_postgresql.sql도 적용해야 한다.
이 SQL은 최근 5년 tb_log/tb_event의 수정·삭제를 막으며 기존 자료를 삭제하지 않는다.

정상 실행 환경에서 주기적으로 강제 기록하는 구현이며, OS 정지·디스크 고장·
프로세스 강제 종료·전원 차단 시에도 정확히 1초 안에 저장된다는 보장은 아니다.
실제 지연은 LOG_INTERVAL_GAP, 파일 기록 실패는 LOG_PERSISTENCE_FAILURE로 드러낸다.
장기 보관용량 산정, 외부 백업·복구 시험, 시각 동기화 및 관리자/OS 직접 삭제 통제는
운영 환경에서 별도로 갖춰야 한다. 기존 정책으로 이미 삭제된 기록은 복구되지 않는다.

검증: SQLite 시험 모드에서 python -m unittest discover -s tests -v.
별도 PostgreSQL 보관 정책 시험은 localhost hdcs_test_* DB만 허용한다.
