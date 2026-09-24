# 0.1.0에서 0.2.0으로 안전하게 옮기는 실습

[목차](README.md) · [변경 지도](changes-v0.2.md) · [59장](59-host-migration.md)

이 실습은 **학습용 복사본**과 자동 테스트가 만든 임시 DB만 사용한다. 제품 운영 DB를 대상으로 명령을 실행하지 않는다. 소스 migration과 저장 데이터 migration은 다른 작업이다.

## 1. 소스 호환성 변화부터 읽기

| 0.1.0 Host의 자리 | 0.2.0에서 할 일 | 이유 |
| --- | --- | --- |
| AgentBindings literal | `interruption_policy: None` 또는 versioned binding 제공 | core 상태와 앱 중단 정책 분리 |
| ModelBinding literal | `default_options` 제공 | Binding→Profile→Run precedence |
| RunRequest literal | optional `max_output_tokens` 처리 | 추론 옵션과 출력 cap 구분 |
| StateStore 구현 | `ExecutionTransactions` 원자 계약 구현 | 명령·segment·lease 절반 저장 방지 |
| RunStatus/OutcomeResult match | `Interrupted` nonterminal 처리 | stop을 cancel과 구별 |
| handle outcome | 해당 segment의 결과로 해석 | 재개 후에도 과거 관찰 불변 |
| 재개 reviewer | 원 execution principal/grant와 분리 | 승인자 계정으로 실행 주체가 바뀌지 않음 |
| model projector | ToolSet·compiled tools·provenance와 saved authorization 제공 | 준비된 입력과 실행 Tool 결합 |
| Gemini/Vertex encode 반환값 | JSON bytes를 HTTP body로 직접 전송 | raw 값의 숫자 정밀도·서명 보존 |
| PolicyGate::run_view | 주입 Clock 인자 제공 | 만료 관찰은 읽기만 수행 |

새 required 필드에 빈 값을 넣어 compiler를 통과시키는 것으로 끝내지 않는다. 실제 권한·state 계약에 맞게 구성하고 [완성 Host](host-example.md)와 비교한다.

## 2. 제출 JSON을 직접 비교하기

동봉한 작은 실행 예제는 원문 숫자와 options의 missing/empty 차이를 확인한다. `ko/` 폴더에서 `COURSE`를 지정한 상태로 실행한다.

```sh
cargo +1.98.1 run --manifest-path "$COURSE/examples/contracts-v02/Cargo.toml" --locked
```

기대 출력은 `v0.2 contract lab: snapshot identity and shallow option precedence passed`다. [전체 코드](examples/contracts-v02/src/main.rs)를 읽고 다음을 설명한다.

- 같은 큰 숫자의 객체 key 순서와 빈 options 여부는 왜 비교가 같을 수 있는가?
- 큰 정수의 마지막 자릿수가 다르면 왜 digest가 달라져야 하는가?
- null은 왜 빈 map이 아닌가?
- nested option의 y는 override 후 왜 없어지는가?

이 예제는 pure contract를 직접 호출한다. 실제 Agent admission·권한·provider 검증을 실행하는 예제는 [Host 실습](walkthrough.md)이다.

## 3. 저장 형식 이관의 실제 테스트 읽기

완성 workspace에서 실행한다.

```sh
cargo test -p wickle-state-sqlite --test execution_store --locked
cargo test -p wickle-state-sqlite --test state_store --locked
```

[SQLite 실행 저장 검사](../reference/crates/wickle-state-sqlite/tests/execution_store.rs)에서 legacy fixture 생성·읽기 전후 DB 값·새 admission을 추적한다. [저장소 검사](../reference/crates/wickle-state-sqlite/tests/state_store.rs)의 `killing_an_uncommitted_legacy_upgrade_preserves_old_data_and_allows_one_later_upgrade`를 읽는다.

기대 행동은 다음과 같다.

1. 과거 terminal outcome/event를 읽어도 DB가 바뀌지 않는다.
2. 과거 active history가 부족한 scope에는 새 admission을 허용하지 않는다.
3. terminal만 있는 scope에서 허용된 새 admission은 원래 terminal을 보존하고 v2로 바뀐다.
4. 미확정 SQLite transaction의 child가 종료돼도 원래 image는 보존된다.
5. 다시 실제 API로 admission하면 한 번 upgrade하고 같은 요청은 중복 생성하지 않는다.

위 kill 검사는 production migration 함수 내부의 특정 코드 행을 직접 kill한 것이 아니다. 실제 엔진으로 생성한 image의 SQLite transaction rollback과 공개 API 재시도를 조합한 시험이다. 이를 정확히 설명하는 것도 평가 항목이다.

## 4. 운영 이관 절차를 학습용 표로 작성하기

| 단계 | 먼저 확인할 사실 | 실패하면 할 일 |
| --- | --- | --- |
| intake 중지 | schedule 포함 신규 요청이 멈췄는가 | 아직 신규 요청이 있으면 작업 중단 |
| 구 runtime drain | waiting·unknown 포함 nonterminal을 해결했는가 | 새 segment를 발명하지 않고 원 runtime으로 처리 |
| worker 정지·일관 backup | WAL을 포함한 일관된 이미지인가 | 단순 live DB 파일 복사로 대체하지 않음 |
| 복사본 검증 | terminal·이벤트가 그대로 읽히는가 | 원본 보존, 원인 확인 |
| 새 Host 실행 | 새 계약으로 요청·승인·복구가 동작하는가 | 교정 후 복사본에서 재검사 |
| 신규 intake 재개 | 동일 태그 crate·runtime인가 | 혼합 버전 운영 방지 |

`execution.legacy_drain_required`, `execution.legacy_checkpoint`, `prepared.legacy_step_boundary` 같은 오류를 지우거나 record를 삭제해 강제 재개하지 않는다. 부족한 과거 근거를 새 기본값으로 대체하면 안전 계약을 잃는다.

SQLite backup의 구체적 명령은 [릴리스 이관 문서](../reference/docs/migration-v0.2.md)에 있다. 이 교재의 자동 검사는 실제 운영 데이터에 backup·upgrade를 수행하지 않는다.

## 5. downgrade와 외부 효과

v2 기록 후 구 binary가 읽을 수 있다고 가정하지 않는다. compatible backup으로 돌아가더라도 backup 이후의 이메일·결제·문서 발행은 DB 복원으로 취소되지 않는다. 해당 외부 시스템의 effect를 확인해야 한다. 이관을 ‘새 필드 추가’ 정도로 설명하지 않는 이유다.

## 완료 조건

컴파일 오류를 모두 해결하고, pure contract 예제와 실제 package Host를 실행하고, 위 저장소 시험에서 read-only·atomic upgrade·active drain·Unknown 보존을 설명한다. 구 runtime과 새 runtime을 동시에 같은 active store에 연결하지 않는다.
