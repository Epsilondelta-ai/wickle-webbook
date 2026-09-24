# 32장. MCP stdio 도구와 프로세스 관리

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 새 MCP 병렬 schema 엔진을 만들지 않고 실제 Agent→binder→stdio 전체 경로를 검증한다. invalid proposals에서 remote 실행 0회, 불확실한 쓰기 재실행 금지를 유지한다.

이어지는 구현: [57장](57-mcp-repair.md).

[목차](README.md) · [이전 장](31-xai.md) · [다음 장](33-events.md)

## 이번 장의 출발점과 결과

31장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **MCP stdio 도구와 프로세스 관리**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/32-mcp.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/32-mcp.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch15-03-drop.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

MCP는 외부 도구 서버와 통신하는 프로토콜이다. 원격 도구를 발견했다고 곧바로 실행 허용하는 것은 아니다. 발견한 schema를 검토하고 어떤 인자를 모델에 맡길지 결정한 뒤 기존 SchemaCompiler/InputBinder/PolicyGate 경로로 연결한다. 이 버전은 tools/stdio의 2025-06-18 부분집합을 구현한다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 32
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. 절대 executable 경로, literal args, 명시적 환경을 가진 McpCommand를 만든다. shell을 거치지 않고 상속 환경을 비운다.

2. initialize와 bounded discovery로 snapshot을 만든다. 이름·서버 identity·schema를 보존하고 선택된 도구에 승인 descriptor를 만든다.

3. 실행 전에 selected descriptor를 다시 확인한다. 새로 발견된 도구가 자동 활성화되거나 변경된 schema가 조용히 대체되지 않게 한다.

4. cancel/timeout/abandon 시 session을 무효화하고 child 종료·reap·SDK cleanup을 추적한다. 쓰기 도구의 성공 응답만으로 Applied를 선언하지 않는다.

## 실제 코드 읽기

`crates/wickle-mcp/src/client.rs`의 이 단계 36–67행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/32-mcp.md)에 있다.

```rust
pub struct McpCommand {
    /// Absolute executable path. No shell interpretation or PATH lookup.
    pub program: PathBuf,
    /// Literal argument vector.
    pub args: Vec<String>,
    /// Explicit environment only; parent environment is cleared.
    pub env: BTreeMap<String, String>,
    /// Optional explicit working directory.
    pub current_dir: Option<PathBuf>,
}
impl fmt::Debug for McpCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("McpCommand(<protected>)")
    }
}
/// Finite protocol, discovery and cleanup bounds.
#[derive(Clone, Debug)]
pub struct McpLimits {
    /// Maximum one inbound or outbound JSON-RPC frame.
    pub max_frame_bytes: usize,
    /// Maximum inbound messages between explicit operations, including notifications.
    pub max_messages: usize,
    /// Maximum discovered tools across pages.
    pub max_tools: usize,
    /// Maximum metadata pages; repeated cursors also fail.
    pub max_pages: usize,
    /// Additional upper bound on initialization.
    pub connect_timeout: Duration,
    /// Additional upper bound on closing and reaping the child.
    pub close_timeout: Duration,
}
impl Default for McpLimits {
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

MCP Adapter는 외부 프로세스를 core ToolExecutor로 감싼다. 공통 도구 계약을 재사용해 새 실행 경로의 권한 누락을 줄인다. 반면 원격 metadata와 효과 receipt의 신뢰 수준이 제한된다. Rust 타입 안전성과 child 실행만으로 OS sandbox가 생기는 것은 아니다. sampling·HTTP·OAuth 등 미구현 기능을 전체 MCP 지원으로 포장하지 않는다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test -p wickle-mcp --test stdio --locked
python3 "$COURSE/lab.py" check 32 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- `crates/wickle-mcp/tests/factory.rs` → `segment_bound_exports_close_and_reopen_without_reusing_old_executors`
- `crates/wickle-mcp/tests/factory.rs` → `unselected_observer_exports_and_invalid_bindings_never_spawn_a_process`
- `crates/wickle-mcp/tests/stdio.rs` → `reviewed_snapshot_keeps_original_names_versions_and_explicit_input_ownership`
- `crates/wickle-mcp/tests/stdio.rs` → `selected_descriptor_drift_blocks_dispatch_and_new_tools_do_not_activate`

### 결함을 주입하는 연습

discovery 후 서버 descriptor를 변경하고 호출하라. 원격 write가 success를 반환했지만 신뢰된 효과 증거가 없을 때 결과를 검사하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

descriptor drift는 dispatch 전에 막힌다. write는 success 응답이어도 Unknown이고 자동 재시도하지 않는다. 원격 readOnly annotation은 힌트이며 Host의 분류·권한 검토를 대신하지 않는다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 32 --dest ../wickle-answer-32
python3 "$COURSE/lab.py" compare 32 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/32-mcp.patch"
git apply "$COURSE/solutions/32-mcp.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `1c4edaa4a5123c8819f319b6e78e11c750f3a40d`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `Cargo.lock`
- `Cargo.toml`
- `README.de.md`
- `README.es.md`
- `README.fr.md`
- `README.ja.md`
- `README.ko.md`
- `README.md`
- `README.ru.md`
- `README.zh-CN.md`
- `crates/wickle-mcp/Cargo.toml`
- `crates/wickle-mcp/src/client.rs`
- `crates/wickle-mcp/src/executor.rs`
- `crates/wickle-mcp/src/factory.rs`
- `crates/wickle-mcp/src/lib.rs`
- `crates/wickle-mcp/src/snapshot.rs`
- `crates/wickle-mcp/src/transport.rs`
- `crates/wickle-mcp/tests/factory.rs`
- `crates/wickle-mcp/tests/stdio.rs`
- `crates/wickle-mcp/tests/support/binding.rs`
- `crates/wickle-mcp/tests/support/mod.rs`
- `docs/mcp.md`
- `scripts/check-package.py`
- `tests/support/mcp_consumer.rs`
- `tests/support/mcp_fixture.rs`

</details>
