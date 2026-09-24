# 0.2.0 Host를 실행하며 전체 흐름 추적하기

[목차](README.md) · [전체 Host 코드](host-example.md)

## 시작하기

새 edition의 ko 폴더에서 `export COURSE="$PWD"`를 실행한다. 다음 명령은 기존 실습을 덮어쓰지 않는 새 폴더에 최종 소스를 복원한다.

```sh
python3 "$COURSE/lab.py" snapshot 60 --dest ../wickle-lab-v02
cd ../wickle-lab-v02
export CARGO_INCREMENTAL=0
python3 scripts/check-package.py --allow-dirty --consumer agent
```

이 명령은 원본 workspace 안에서 fake를 직접 호출하는 데 그치지 않는다. 라이브러리를 package한 뒤 별도 임시 workspace에 추출하고 공개 interface로 Host를 실행한다. 처음에는 dependency 다운로드와 C 빌드 도구가 필요하다. 모델 key와 유료 inference는 사용하지 않는다. 선택 consumer 통과는 전체 suite 통과가 아니다.

## Host main의 조립 순서

1. scope·SQLite·모델 catalog와 primary/fallback route를 만든다.
2. policy·metadata inspector·model port·estimator를 주입한다.
3. Binding 기본 옵션 → Profile → Run의 effective 옵션과 출처를 설정한다.
4. versioned MaintenancePolicy와 AppStateSchema를 제공하고 Agent를 만든다.
5. start·outcome·events·duplicate replay를 실행한다.
6. HostShutdown 중단 후 저장된 app_state와 Interrupted outcome을 확인한다.
7. snapshot에서 recovery_record를 만들어 명시적으로 Recover한다.
8. 새 handle 성공, 옛 handle Interrupted 유지, 같은 command의 추가 호출 0을 검사한다.
9. inspect_step에서 저장 근거와 redaction을 확인하고 조회 전후 외부 호출·저장 변경이 없는지 검사한다.

`agent consumer:` 출력과 종료 코드 0을 확인한다. 정확한 세부 assertion은 전체 코드에 있으며 결과를 미리 정해 놓은 텍스트의 존재만 검사하지 않는다.

## 역할을 분리해서 보기

| 객체 | 의미 |
| --- | --- |
| RequestSnapshot | 원래 제출값과 비교 규칙 |
| ModelConfiguration | route 선택 후 확정한 옵션·출처·상한 |
| PreparedStep | 이번 모델에 보낼 의미와 도구·문맥 계약 |
| physical invocation | 한 번의 실제 시도에 대한 예약·관찰 |
| ExecutionSegment | 한 실행 구간과 그 구간의 고정된 outcome |
| ControlReceipt | 명령 수락/처리 근거; 자체로 업무 완료는 아님 |
| CompositionReport | 재실행 없이 조회한 저장된 구성의 공개 view |

## 도구 입력과 재개

```sh
python3 scripts/check-package.py --allow-dirty --consumer tool_schema
python3 scripts/check-package.py --allow-dirty --consumer resume
python3 scripts/check-package.py --allow-dirty --consumer execution_contract
```

schema 예제에서 strict wire의 optional 생략이 canonical 생략으로 복원된 뒤 default 10이 되는 경로를 추적한다. resume 예제에서는 reviewer와 원래 실행자를 구별하고 system 값이 바뀌지 않는지 본다. 최신 consumer 선택지는 `python3 scripts/check-package.py --help`로 확인한다.

## 전체 검증

```sh
python3 scripts/check-package.py --allow-dirty
```

전체 검사에는 다른 예제와 별도의 두 business workspace가 포함된다. 선택 agent만 통과해도 include된 공유 타입을 쓰는 recovery 소비자가 깨질 수 있다. 마지막에는 전체 검사를 수행한다. 0.1.0 main.rs를 새 Cargo dependency에 그대로 붙이지 않고 [이관 실습](migration-lab.md)으로 필드와 Port 계약을 갱신한다.
