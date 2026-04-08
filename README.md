# CAN Tools

BLE/Classic BT OBD 어댑터를 통한 CAN 버스 도구 모음. macOS에서 동작하며, ELM327/STN2255 호환 어댑터(OBDLink MX+, vLinker MC+ 등)를 지원한다.

## 도구

| 도구 | 설명 |
|------|------|
| **can_reader** | OBD-II PID 조회, CAN 모니터링, 버스 감지, 시그널 탐색 |
| **can_discover** | PT-CAN 시그널 자동 탐색 전용 (경량, 대화형) |

`can_reader`는 D-CAN/PT-CAN 양쪽을 지원하는 다목적 도구이고, `can_discover`는 PT-CAN 시그널 탐색에 특화된 단일 목적 도구이다. `can_reader discover` 명령과 `can_discover`는 동일한 탐색 로직(`can_discover.c`)을 공유한다.

## 빌드

```bash
./build.sh
```

또는:

```bash
mkdir -p build && cd build && cmake .. && make
```

빌드 후 `build/libcan_discover_core.a`가 함께 생성된다. 다른 앱에서 탐색 엔진만 쓰고 싶으면
`common/can_discover.h`를 포함해 `disc_analyze_with_raw()`와 `disc_render_draft_dbc()`를 직접 호출하면 된다.

## can_reader

D-CAN(OBD 포트) 액티브 쿼리와 PT-CAN 패시브 모니터링을 모두 지원한다.

### CLI 모드

```bash
# BLE 스캔
./build/can_reader scan

# D-CAN vs PT-CAN 자동 감지
./build/can_reader detect OBDLink

# 지원 PID 목록 (D-CAN)
./build/can_reader supported OBDLink

# PID 값 조회 (D-CAN)
./build/can_reader query OBDLink 0C 0D 11

# CAN 패시브 모니터 (PT-CAN, 10초)
./build/can_reader monitor vLinker 10

# CAN 모니터 + ID 필터
./build/can_reader monitor vLinker 10 0A5 1A0 316

# PT-CAN 시그널 자동 탐색
./build/can_reader discover vLinker

# 원시 AT 명령
./build/can_reader raw OBDLink "ATRV"
```

- JSON 출력은 stdout, 로그는 stderr. `2>/dev/null`로 JSON만 파싱 가능.
- `<device>`: BLE 이름 prefix 매칭 (대소문자 무시)

### 대화형 모드

인자 없이 실행하면 대화형 메뉴 모드로 진입:

```bash
./build/can_reader
```

## can_discover

PT-CAN 시그널 자동 탐색 전용 도구.

```bash
# 특정 어댑터 지정
./build/can_discover vLinker

# 첫 번째 어댑터에 자동 연결
./build/can_discover
```

### 동작 흐름

대화형 6단계 캡처로 모터스포츠 핵심 시그널을 자동 식별한다:

```
Phase 1: Baseline     (5초)  — 기본 트래픽 캡처
Phase 2: Steering     (8초)  — 스티어링 풀 좌/우 반복
Phase 3: Throttle     (8초)  — 액셀 끝까지 밟기/떼기 반복
Phase 4: Brake        (8초)  — 브레이크 끝까지 밟기/떼기 반복
Phase 5: Gear        (10초)  — P → R → N → D 시프트 [스킵 가능]
Phase 6: Wheel Speed (15초)  — 저속 주행 30+ km/h [스킵 가능]
```

각 Phase 후 감지 결과를 신뢰도와 함께 표시하고, 포함/제외를 선택한다.

완료 시 JSON 결과와 함께 작업 디렉터리에 `ptcan_discover_draft_YYYYMMDD_HHMMSS.dbc`
형식의 draft DBC 파일도 생성한다. 이 파일은 raw 값 기준의 초안이며,
내장된 prior table과 exact CAN ID가 맞으면 signal layout/scale/unit을 보강한다.
prior가 없더라도 raw frame을 다시 스캔해 bitfield 길이/시작 비트를 보정하고,
counter처럼 보이는 증가 필드를 억제한다. wheel speed는 가능한 경우 4개 신호로 분해해 출력한다.
discover 실행 시에는 기본적으로 `captures/YYYYMMDD_HHMMSS/` 세션 폴더를 만들고,
각 phase raw dump(`*.frames.tsv`), `manifest.json`, `result.json`, `ptcan_discover_draft.dbc`를 함께 저장한다.
원하면 `--dump-dir <DIR>`로 저장 위치를 덮어쓸 수 있다.
이 prior table은 `tools/gen_can_discover_priors.py`로 `dbcs/*.dbc`에서 생성하며, 런타임에는 `dbcs/`가 없어도 된다.
그래도 checksum/counter와 나머지 bit-packed field는 사람이 검토해 보정해야 한다.

### D-CAN / PT-CAN 자동 감지

Baseline 캡처의 통계(전체 CAN ID의 median Hz)로 버스 접속 모드를 자동 판별한다:

| 모드 | 판정 조건 | Hz 임계값 | 경로 |
|------|----------|-----------|------|
| **DIRECT** (PT-CAN) | median Hz ≥ 10 | 30.0 Hz | 어댑터 → PT-CAN 직결 |
| **GATEWAY** (D-CAN) | median Hz < 10 | 1.0 Hz | 어댑터 → OBD 포트 → ZGW → PT-CAN |

D-CAN 경유 시 ZGW가 PT-CAN 메시지를 저속으로 중계하기 때문에 모든 신호의 Hz가 낮아진다(0.1~5 Hz). GATEWAY 모드가 감지되면 RPM·wheel_speed의 confidence 판정 임계값이 자동으로 낮아져, D-CAN에서도 HIGH confidence를 받을 수 있다.

```
[INFO] Bus mode: GATEWAY (D-CAN)    ← median Hz ~0.7
[INFO] Bus mode: DIRECT (PT-CAN)    ← median Hz ~50
```

JSON 출력에도 `"bus_mode"` 필드가 포함된다.

### 시그널 특성화

캡처 중 풀 레인지 입력으로 데이터 타입도 함께 분석:

- **Endianness**: Big/Little-endian (2바이트 시그널)
- **Signedness**: Signed/Unsigned
- **Raw Range**: 관측 최솟값/최댓값
- **Confidence**: HIGH/MEDIUM/LOW

### 출력 예시

```json
{
  "bus_mode":"direct",
  "dbc_path":"ptcan_discover_draft_20260408_153012.dbc",
  "steering":{"can_id":"0x0A5","dlc":8,"hz":100.0,"byte":2,"byte2":3,"score":48.5,"confidence":"HIGH","endian":"big","signed":true,"raw_min":-5420,"raw_max":5380},
  "rpm":{"can_id":"0x316","dlc":8,"hz":50.0,"byte":2,"byte2":3,"score":35.2,"confidence":"HIGH","endian":"big","signed":false,"raw_min":780,"raw_max":6200},
  "throttle":{"can_id":"0x1A0","dlc":8,"hz":50.0,"byte":5,"score":28.0,"confidence":"HIGH","signed":false,"raw_min":0,"raw_max":255},
  "brake":{"can_id":"0x1A0","dlc":8,"hz":50.0,"byte":4,"score":22.0,"confidence":"HIGH","signed":false,"raw_min":0,"raw_max":180},
  "wheel_speed":{"can_id":"0x1A6","dlc":8,"hz":50.0,"byte":0,"byte2":1,"score":42.0,"confidence":"HIGH","endian":"big","signed":false,"raw_min":0,"raw_max":892}
}
```

## 프로젝트 구조

```
can_tools/
├── common/              # 공유 라이브러리
│   ├── elm327.c/h       # ELM327 프로토콜 핸들러
│   ├── stn2255.c/h      # STN2255 확장 (배치 쿼리, STMA 등)
│   └── can_discover.c/h # PT-CAN 시그널 자동 탐색 엔진
├── can_reader/          # 다목적 CAN 도구
│   └── main.m
├── can_discover/        # 시그널 탐색 전용 도구
│   └── main.m
├── CMakeLists.txt
└── build.sh
```

## 주의사항

- macOS Bluetooth 권한 필요 (시스템 설정 > 개인정보 보호 및 보안)
- PT-CAN 시그널 탐색은 PT-CAN 직결 및 D-CAN(OBD 포트) 경유 모두 지원 — 버스 모드 자동 감지
- 어댑터는 패시브 모드에서 Silent Mode(`ATCSM1`)로 동작 — CAN ACK 미전송
- IGN ON 상태에서 실행해야 함
