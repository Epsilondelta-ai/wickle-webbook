# 36장. 0.1.0 완성·패키징·최종 평가

이 장은 빈 프로젝트에서 만드는 **0.1.0 기초 checkpoint**다. 해당 단계의 코드를 그대로 구현한 뒤 37–60장에서 0.2.0으로 발전시킨다. 최종 API를 이 단계에 섞지 않는다.

**0.2.0에서 달라지는 점:** 이 장은 기초 0.1.0 checkpoint 완성이다. 전체 학습 목표인 0.2.0은 37–60장을 완료한 뒤 최종 평가한다.

이어지는 구현: [37장](37-compatibility.md) · [59장](59-host-migration.md) · [60장](60-release-v02.md).

[목차](README.md) · [이전 장](35-integration.md) · [다음 장](37-compatibility.md)

## 이번 장의 출발점과 결과

35장 구현과 검사를 마친 실습 폴더에서 이어서 작성한다. 이번 장에서는 **0.1.0 완성·패키징·최종 평가**를 구현한다. 본문은 원리를 설명하고, [전체 구현·테스트](implementation/36-release.md)는 모든 변경 Rust 파일의 완성본을 제공한다. [정답 패치](solutions/36-release.patch)에는 Cargo.toml·Cargo.lock·문서 변경까지 포함되어 있다.

Rust 선행 읽기: [The Rust Programming Language 관련 장](https://doc.rust-lang.org/book/ch14-00-more-about-cargo.html). 필요한 문법을 먼저 [Rust 기초](01-rust.md), [비동기 Rust](02-async.md), [Book 대응표](rust-book-map.md)에서 익힌다. 아래 Wickle 동작과 설계 해석의 근거는 이 장의 실제 코드와 테스트다.

## 강의: 문제를 데이터와 동작으로 나누기

완성의 기준은 원래 0.1.0의 계약과 공개 package가 함께 작동하는 것이다. Rust 파일을 모두 작성했다는 것만으로 release를 재현한 것은 아니다. Cargo manifest/lockfile, tests/consumer, license, 지원 범위 문서까지 같은 결과를 설명해야 한다.

## 구현 실습

터미널은 00장에서 만든 `wickle-lab`에 둔다. `COURSE`는 교재 디렉터리의 절대 경로다. 먼저 이 장에서 바뀌는 파일을 확인한다.

```sh
python3 "$COURSE/lab.py" inspect 36
```

출력의 변경 파일을 대상으로 아래 순서로 작성한다. 처음에는 테스트의 입력과 기대값을 읽고, 구현을 작성한 뒤 전체 코드와 비교한다. `git diff`의 `-`는 이전 코드, `+`는 새 코드, 나머지는 위치를 찾는 문맥이다. 이를 모두 새 파일에 붙여 넣으면 안 된다.

1. 마지막 patch로 release manifest·MIT LICENSE·설치 문서와 공개 표면을 완성한다. publish=false이므로 crates.io publish를 실습 단계로 넣지 않는다.

2. fmt, workspace check/clippy/test/doc와 extracted package consumer를 실행한다. core 최소 Rust 지원과 개발 toolchain을 구별한다.

3. 전체 참조 파일 비교를 수행한다. 다른 구현이라면 byte 동일성 대신 모든 계약 테스트와 불변 조건 설명으로 동등성을 평가하되 차이를 기록한다.

4. 아래 최종 평가 시나리오를 수행하고 자동 검사와 직접 consumer 실행 결과를 별도로 기록한다. 실제 API를 호출하지 않은 것을 live 성공으로 보고하지 않는다.

## 실제 코드 읽기

`scripts/check-package.py`의 이 단계 1–32행이다. 아래 블록은 **읽기용 발췌**이므로 독립 프로그램이 아니다. 실행 가능한 전체 파일은 [구현 문서](implementation/36-release.md)에 있다.

```rust
#!/usr/bin/env python3
"""Verify the core dependency boundary and consume extracted Cargo packages."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent.parent
# Changes to core dependencies require an explicit boundary review.
CORE_DEPENDENCIES = {
    "futures-util", "getrandom", "jsonschema", "serde", "serde_json", "sha2", "thiserror",
    "tokio", "tokio-util",
}


def source_digest(directory):
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(directory).as_posix().encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
```

선언을 읽을 때 세 가지를 표시한다. 누가 값을 소유하는가(`self`, `&self`, `Arc`), 누가 실패를 처리하는가(`Result`, `?`), 무엇을 저장하고 무엇을 다시 구성하는가(직렬화 데이터와 runtime 객체). 이어서 같은 파일의 `impl`을 따라가며 검증보다 외부 호출이 먼저 일어나는 경로가 있는지 확인한다.

## 소프트웨어 공학: 구조의 이유와 비용

Release engineering도 아키텍처의 일부다. 다중 crate 구조는 선택 설치라는 장점을 주지만 모두 같은 release로 조합되는지 검증해야 한다. MIT license와 재현 가능한 소스 패키지는 배포 계약이다. 0.1.0의 한계를 문서화하는 것은 기능 부족을 숨기는 대신 Host가 맡을 책임을 명확히 하는 일이다.

[아키텍처 강의](02b-architecture.md)의 패턴 이름은 이 코드를 이해하는 도구다. 이름을 맞히는 것보다 이 경계가 없어지면 어느 테스트와 업무 시나리오가 깨지는지 설명하는 것이 목표다.

## 검증: 성공뿐 아니라 금지된 동작도 관찰하기

```sh
cargo test --workspace --locked
python3 "$COURSE/lab.py" check 36 --work .
```

두 명령은 같은 장 검사를 실행하는 직접 방식과 helper 방식이다. 한 가지를 실행하면 된다. `test result: ok`와 실패 0을 확인하고 실행된 테스트 이름·개수가 0이 아닌지도 본다. 초기 빈 라이브러리인 03장은 예외이며 이후 장의 행동 검증으로 확장한다. 실행하지 않은 검사를 통과했다고 기록하지 않는다.

읽을 테스트:

- workspace 테스트와 아래 독립 소비자 검사 결과를 확인한다.

### 결함을 주입하는 연습

서버·UI 없이 도구를 한 번 쓰는 Host를 실행하고 같은 요청 replay, 승인 대기 재개, 쓰기 직후 crash 복구, 외부 이벤트 메모리 적용까지 시연하라. 각 경계에서 누가 권위 있는 상태를 갖는지 설명하라.

수정 전 성공 → 의도한 결함을 넣었을 때 실패 → 결함을 제거한 뒤 성공의 세 결과를 기록한다. 저장 복구·효과 테스트는 단순 오류 문자열뿐 아니라 callback 횟수, revision, 저장된 효과를 함께 본다. 새로운 결함 실험을 다음 장으로 가져가지 않는다.

<details>
<summary>연습 해설 — 먼저 직접 예측한 뒤 열기</summary>

정답은 단순 답변 문자열이 아니다. 요청 중복 억제, 권한 철회, 고정 인자, 예산, unknown 보존, lease fencing, durable outcome, 독립 package 사용을 실제 관찰해야 한다. Hosted SaaS·노코드 UI·운영 worker·분산 queue는 이 release에 없는 Host 제품 책임이다.

</details>

## 정답 비교와 막혔을 때의 복구

직접 작성한 코드를 보존한 채 별도의 참조 폴더를 만든다. 목적지는 아직 존재하지 않아야 한다.

```sh
python3 "$COURSE/lab.py" snapshot 36 --dest ../wickle-answer-36
python3 "$COURSE/lab.py" compare 36 --work .
```

`compare`는 정답과 다른 참조 파일 이름을 출력하며 차이가 있으면 종료 코드 1이다. 이것만으로 오답이라는 뜻은 아니다. 동등한 구현도 다른 bytes를 가질 수 있으므로 행동 테스트와 함께 판단한다. 추가한 학습 메모 등은 비교 대상이 아니다. 이전 단계와 **완전히 같은 참조 구현**에서 정답을 적용하려는 경우에만 다음 두 명령을 쓴다. 직접 구현한 코드에는 충돌할 수 있으므로 먼저 commit하거나 별도 복사한다.

```sh
git apply --check "$COURSE/solutions/36-release.patch"
git apply "$COURSE/solutions/36-release.patch"
```

패치가 맞지 않으면 `--reject`로 억지 적용하지 말고 이전 장 기준인지 확인한다. Rust import 오류는 `lib.rs`의 `mod`와 `pub use`, manifest의 workspace member와 dependency부터 확인한다. 테스트가 끝나지 않으면 실제 시계와 가짜 시계를 혼용하지 않았는지, 생성한 task/child 종료를 기다리고 있는지 확인한다.

## 다음 장으로 넘어가는 기준

구현 검사가 성공하고, 연습의 실패 원인과 위 설계의 장점·비용을 자신의 말로 설명할 수 있어야 한다. 코드의 핵심 흐름을 입력 → 검증 → 상태 변경 → 외부 효과 → 저장 순서로 그린다. 이 장의 정확한 기준 commit은 `117b141ad2505a26c5175405520003a5ea0ab32d`이며 최종 0.1.0 소스와 중간 단계의 API가 다를 수 있다.

<details>
<summary>이 장의 전체 변경 파일 목록</summary>

- `CHANGELOG.md`
- `CONTRIBUTING.md`
- `Cargo.toml`
- `LICENSE`
- `README.de.md`
- `README.es.md`
- `README.fr.md`
- `README.ja.md`
- `README.ko.md`
- `README.md`
- `README.ru.md`
- `README.zh-CN.md`
- `crates/wickle-adapter-runtime/Cargo.toml`
- `crates/wickle-adapter-runtime/LICENSE`
- `crates/wickle-mcp/Cargo.toml`
- `crates/wickle-mcp/LICENSE`
- `crates/wickle-model-anthropic/Cargo.toml`
- `crates/wickle-model-anthropic/LICENSE`
- `crates/wickle-model-azure-openai/Cargo.toml`
- `crates/wickle-model-azure-openai/LICENSE`
- `crates/wickle-model-bedrock/Cargo.toml`
- `crates/wickle-model-bedrock/LICENSE`
- `crates/wickle-model-gemini/Cargo.toml`
- `crates/wickle-model-gemini/LICENSE`
- `crates/wickle-model-openai/Cargo.toml`
- `crates/wickle-model-openai/LICENSE`
- `crates/wickle-model-responses/Cargo.toml`
- `crates/wickle-model-responses/LICENSE`
- `crates/wickle-model-router/Cargo.toml`
- `crates/wickle-model-router/LICENSE`
- `crates/wickle-model-vertex/Cargo.toml`
- `crates/wickle-model-vertex/LICENSE`
- `crates/wickle-model-xai/Cargo.toml`
- `crates/wickle-model-xai/LICENSE`
- `crates/wickle-state-sqlite/Cargo.toml`
- `crates/wickle-state-sqlite/LICENSE`
- `crates/wickle/Cargo.toml`
- `crates/wickle/LICENSE`
- `docs/installation.md`
- `scripts/check-package.py`
- `tests/support/report_process_consumer.rs`

</details>
