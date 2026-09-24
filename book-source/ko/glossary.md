# 기술 용어집

[목차](README.md) · [전체 흐름](walkthrough.md)

| 용어 | 이 교재에서의 의미 | 혼동하지 말 것 |
| --- | --- | --- |
| LLM / 모델 | 메시지를 받아 다음 텍스트·도구 제안을 생성하는 구성요소 | 실행 권한의 결정자 아님 |
| Agent | 목표·지시·관찰을 바탕으로 모델과 도구를 연결하는 실행 주체 | 모델 자체, OS 프로세스와 동일하지 않음 |
| Host | Wickle을 포함하고 인증·credential·정책·runtime·UI를 제공하는 애플리케이션 | core 내부에서 자동 생성되지 않음 |
| Profile | 버전이 있는 선언적 에이전트 구성 | client 객체나 비밀키 저장소 아님 |
| Port | core가 정의한 외부 기능의 trait 계약 | TCP port 번호가 아님 |
| Adapter | 외부 provider/protocol/store를 Port로 연결하는 구현 | 권한 우회 경로 아님 |
| Binding | 정확한 정의와 연결·버전을 묶은 구성 | 문자열 이름 하나만으로 충분하지 않음 |
| Scope | tenant/workspace/선택 user로 된 자원 공간 | None을 wildcard로 쓰지 않음 |
| Principal / grant | 현재 행위자와 현재 권한 근거 | 자원의 owner namespace와 다름 |
| Session | prompt identity와 transcript를 공유하는 대화 묶음 | 물리 HTTP connection과 다름 |
| Run | 중복 식별 가능한 요청 한 건의 논리적 실행 | 승인 재개 때 반드시 새 Run을 만드는 것은 아님 |
| Segment | 대기·재개 등으로 나뉜 한 실행 구간 | Run 전체와 수명이 같지 않음 |
| Logical step | 하나의 모델 판단 단위 | retry의 각 HTTP 요청과 다름 |
| Physical attempt | 실제 호출 한 번 또는 명시적인 시도 예약 identity | 성공한 호출만 세지 않음 |
| Tool call | 모델이 제안하고 엔진이 검증할 행위 의도 | 제안되었다고 자동 실행되지 않음 |
| Tool observation | 모델에 돌려줄 제한된 결과 | 보호 receipt·실행 인자 전체가 아님 |
| System input | 인증된 Host 또는 resolver가 공급하는 값 | 모델이 생성할 텍스트가 아님 |
| Bound input | 특정 call에 고정하고 저장한 최종 인자 | resume 때 최신 값으로 재조회하지 않음 |
| Schema | 자료형·필수 필드·범위를 정의한 구조 계약 | 값의 실재 소유권을 증명하지 않음 |
| Snapshot | 특정 시점의 고정된 데이터와 identity | 현재 접근 권한을 영구 보장하지 않음 |
| Digest | versioned canonical 표현의 해시 | 출처 인증·암호화·권한 토큰이 아님 |
| Revision | 저장 변경의 순서 번호 | 모델 버전과 다름 |
| CAS | 예상 revision이 맞을 때만 저장하는 동시성 검사 | lease의 실행 소유권 검사와 다름 |
| Lease | 제한된 시간 동안의 실행 소유권 | 무기한 distributed lock이 아님 |
| Fencing | 이전 소유자의 늦은 쓰기를 generation으로 거절 | 외부 시스템이 자동으로 함께 준수하는 것은 아님 |
| Idempotency | 같은 의도를 반복해도 중복 효과를 만들지 않는 성질 | 아무 POST나 재전송해도 된다는 뜻 아님 |
| Receipt | 외부 효과를 확인하는 보호된 근거 | 모델의 성공 설명과 다름 |
| Applied / NotApplied / Unknown | 외부 업무 효과가 확인됨 / 미적용 확인 / 불명 | 결과 schema 성공·실패와 독립 |
| Reconcile | 기존 시도의 결과를 조회·조정하여 불확실성을 해소 | 업무를 다시 실행하는 retry가 아님 |
| Transcript | 실제 대화·관찰의 원본 기록 | context 크기를 줄이려고 임의 삭제할 자료 아님 |
| Projection | 특정 모델 호출에 보낼 명시적 표현 | 저장 구조의 무조건 Serialize가 아님 |
| ContextSource | 자동으로 읽기 자료를 조회하는 확장점 | 부작용 있는 업무 Tool과 다름 |
| RAG | 모델 호출에 검색한 근거 자료를 제공하는 방식 | 모델 재학습과 다름 |
| Skill | 버전 고정 지시 본문과 지원 자산 참조 | 그 자체가 도구 실행 capability를 부여하지 않음 |
| Artifact / Evidence | 큰 원본 바이트 / 그 원본의 버전·해시·출처 근거 | preview가 원본 전체는 아님 |
| Compaction | 원본을 유지하며 모델 표현을 축약 | 요약의 사실성을 완전히 증명하는 절차 아님 |
| Verifier | 고정된 기준으로 후보를 판정하는 구성요소 | 모든 업무 진실을 보장하는 만능 판정자 아님 |
| Durable event | 상태와 함께 commit한 순서 있는 사건 | 임시 token delta와 보장 수준이 다름 |
| Outbox / delivery journal | 발견·전달할 사건과 적용 상태를 보존하는 자료 | core가 제공하는 운영 queue 서비스는 아님 |
| Future / Stream | 나중에 하나의 결과 / 여러 항목을 내는 비동기 계산 | 각각 새 OS thread라는 뜻 아님 |
| MSRV | 최소 지원 Rust 버전 | 개발에 고정한 toolchain 버전과 다름 |
| Conformance test | 서로 다른 구현이 같은 계약을 지키는지 검사 | 원격 서비스 live 검증과 다름 |

## 0.2.0 추가 용어

| 용어 | 의미 | 구분할 것 |
| --- | --- | --- |
| RequestSnapshot | 원 제출과 정규화 규칙의 저장 | effective options와 다름 |
| 숫자 lexeme | JSON 숫자의 원래 문자열 표기 | f64의 수치와 다름 |
| ModelConfiguration | effective options·출처·schema revision | endpoint·credential은 inference option이 아님 |
| ProviderToolSchemaCompiler | 모델 소유 계약을 공급자 표현으로 변환 | 숨은 system schema를 전달하지 않음 |
| Presence / JsonText / JsonObjectText | 생략·null·표현 불가 shape의 가역 인코딩 | validation이나 권한 면제가 아님 |
| ToolRepair | 완전 응답의 잘못된 tool round를 수정하는 예약 | transport recovery·업무 verifier repair와 원인이 다름 |
| PreparedStep | route·options·ToolSet·context 입력 고정 | 전송 완료 또는 영구 권한 증거 아님 |
| core_revision | 동일 fragment의 코어 활성화 개정 | 내용 hash나 외부 revision과 다름 |
| Tombstone / lineage | 삭제 관찰 / 자료 파생 관계 | 최신 Empty가 삭제 이력을 취소하지 않음 |
| Interrupted | 복구 가능한 실행 구간 중단 | terminal Cancelled와 다름 |
| AppState | Host namespace·업무 status·metadata | core 상태 전이의 권한이 아님 |
| ControlReceipt | 명령 수락·처리 segment 근거 | remote 전달·업무 완료와 별개 |
| deadline_expired | 읽기 시점의 기한 관찰 | Expire command에 의한 terminal 확정과 별개 |
| CompositionReport | 저장 step 근거의 제한된 view | 현재 자료로 새 실행을 구성하는 것이 아님 |
| TransmissionUnknown | 전송/응답을 입증할 근거 부족 | reservation은 전송 증거가 아님 |
