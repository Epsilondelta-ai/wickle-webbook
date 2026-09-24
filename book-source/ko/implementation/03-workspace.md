# 03장 전체 Rust 구현과 테스트

[강의로](../03-workspace.md) · [전체 변경 패치](../solutions/03-workspace.patch)

기준 `67ba3362310c798b7565aa2125e402fb97a1af2d`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! This crate currently contains the package foundation. Agent execution is
//! not implemented yet.
```

## `tests/support/consumer.rs`

```rust
use wickle as _;

fn main() {}
```
