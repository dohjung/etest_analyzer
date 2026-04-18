# Bin Similarity Monitor — 기술 문서

**파일**: `bin_similarity_monitor.py`  
**작성일**: 2025-04-17  
**대상**: Yield Engineering 팀 동료 엔지니어

---

## 1. 개요

### 1.1 목적

Pass unit(정상 판정된 die)이 특정 Fail bin의 파라미터 분포에 점점 가까워지는 추세를 감지하여 수율 저하 징후를 조기에 경고하는 모니터링 시스템이다.

일반적으로 단일 파라미터의 spec limit 초과 여부만으로는 잠재적 불량을 감지하기 어렵다. 이 시스템은 **3,000개에 달하는 테스트 파라미터를 다변량으로 활용**하여 pass unit이 known fail bin의 분포와 얼마나 유사한지를 정량화하고, 그 추세를 시계열로 추적한다.

### 1.2 분석 대상 데이터

| 항목 | 내용 |
|---|---|
| 데이터 소스 | Impala DB (`test_results` 테이블) |
| 1차 테스트 구분 | `test_mode = 'xp'` |
| 재테스트 구분 | `test_mode = 'xr'` |
| Pass unit 정의 | `soft_bin IN (1, 2)` |
| Fail unit 정의 | `soft_bin NOT IN (1, 2)` |
| 분석 단위 | Lot (추후 Wafer 단위 확장 예정) |

### 1.3 전체 파이프라인

```
[Impala DB]
    │
    ├── Fail unit 추출 (과거 데이터)
    │       └── Fail bin별 Percentile 프로파일 구축 (7일 주기 갱신)
    │
    └── Pass unit 추출 (당일 데이터)
            └── 각 Fail bin과의 유사도 계산 (die 단위)
                    └── Lot 단위 Median 집계
                            └── CUSUM 추세 감지
                                    └── 알람 생성 및 CSV 저장
```

---

## 2. 분석 방법론

### 2.1 Fail Bin 프로파일 — Percentile 기반 분포 정의

각 Fail bin의 "정상 범위"를 과거 fail unit 데이터의 5th ~ 95th percentile로 정의한다.

**선택 이유**: Fail bin당 샘플 수가 10~100개 수준으로 적은 현실에서, 공분산 행렬 추정이 필요한 Mahalanobis Distance는 적용이 불가능하다(feature 수 p=3,000 대비 최소 수만 개의 샘플 필요). Percentile 방식은 feature별로 독립적으로 계산하므로 30개 이상의 샘플에서도 안정적으로 작동한다.

**구축 주기**: 기본 7일마다 재계산하며, 과거 90일치 데이터를 사용한다. 샘플 수가 `MIN_SAMPLES(=10)` 미만인 bin 또는 feature는 프로파일에서 제외한다.

**프로파일 구조**:

```json
{
  "updated_at": "2025-04-17T09:00:00",
  "profiles": {
    "3": {
      "feature_A": { "p_low": 1.23, "p_high": 4.56, "n_samples": 45 },
      "feature_B": { "p_low": 0.01, "p_high": 0.08, "n_samples": 45 }
    }
  }
}
```

### 2.2 유사도 계산 — Percentile 범위 내 Feature 비율

Pass unit 하나에 대해 특정 Fail bin과의 유사도를 다음과 같이 정의한다.

```
유사도 = (Fail bin percentile 범위 내에 있는 feature 수) / (공통 feature 수)
```

- 유사도 범위: 0.0 ~ 1.0
- 유사도 1.0: 모든 파라미터가 해당 fail bin의 분포 범위 내에 위치
- 유사도 0.0: 어떤 파라미터도 해당 fail bin의 분포 범위 내에 없음

**주의**: 유사도가 높다는 것은 해당 die가 fail bin과 파라미터 패턴상 유사하다는 의미이며, 즉각적 불량을 의미하지는 않는다. 추세(trend)의 변화가 핵심이다.

### 2.3 Lot 단위 집계 — Median

Die 단위 유사도를 Lot 단위로 집계할 때 **Median**을 사용한다.

Mean 대신 Median을 사용하는 이유: Lot 내 일부 outlier die(실제 불량 die 등)가 평균값을 왜곡할 수 있기 때문이다. Median은 이러한 outlier에 강건하다.

### 2.4 CUSUM (Cumulative Sum Control Chart) — 추세 감지

CUSUM은 시계열 데이터에서 **점진적인 평균 이동(drift)**을 감지하는 데 특화된 SPC(Statistical Process Control) 기법이다. 단순 threshold 초과 방식과 달리, 작은 변화가 누적될 때 효과적으로 반응한다.

**알고리즘**:

```
z_t = (x_t - μ) / σ          # 표준화 (μ, σ는 baseline으로 추정)

S_pos_t = max(0, S_pos_{t-1} + z_t - k)   # 상향 drift 누적합
S_neg_t = max(0, S_neg_{t-1} - z_t - k)   # 하향 drift 누적합

알람 조건: S_pos_t > h  OR  S_neg_t > h
```

**파라미터 설명**:

| 파라미터 | 기본값 | 의미 |
|---|---|---|
| `k` (slack) | 0.5 | 감지할 최소 drift 크기 (σ 단위). 0.5는 0.5σ 이상의 drift를 감지 |
| `h` (threshold) | 5.0 | 알람 기준값. 클수록 false alarm 감소, 감지 속도 저하 |
| `baseline_n` | 30 | 기준 μ, σ 추정에 사용할 최근 lot 수 |

**Baseline 안정성 검증**: Baseline 구간 유사도의 표준편차가 0.1을 초과하면 경고를 출력한다. Baseline 자체가 drift 중이면 기준값이 오염되어 감지 성능이 저하된다.

**참고**: CUSUM은 단방향(상향 또는 하향) drift를 모두 감지한다. 유사도가 증가하는 방향(pass unit이 fail bin에 가까워짐)뿐 아니라 감소하는 방향도 감지하여 이상 추세 전반을 모니터링한다.

---

## 3. 파일 구조 및 설정

### 3.1 디렉토리 구조

```
bin_similarity_monitor.py   ← 메인 스크립트
data/
  bin_profiles.json         ← Fail bin 프로파일 (자동 갱신)
  similarity/
    similarity_YYYYMMDD.csv ← 날짜별 Lot 유사도
  alarms/
    alarms_YYYYMMDD.csv     ← 날짜별 알람 상세
logs/
  monitor_YYYYMMDD.log      ← 실행 로그 (INFO/WARNING/ERROR)
```

### 3.2 주요 설정값 (`스크립트 상단 상수`)

```python
DB_CONFIG = {
    'host': 'your_impala_host',   # Impala 호스트 주소
    'port': 21050,
    'database': 'your_database',
}

PROFILE_PATH   = './data/bin_profiles.json'
SIMILARITY_DIR = './data/similarity'
ALARM_DIR      = './data/alarms'
LOG_DIR        = './logs'

CUSUM_K        = 0.5    # CUSUM slack
CUSUM_H        = 5.0    # CUSUM 알람 threshold
PERCENTILE_LOW  = 5     # 프로파일 하한 percentile
PERCENTILE_HIGH = 95    # 프로파일 상한 percentile
MIN_SAMPLES    = 10     # 프로파일 구축 최소 샘플 수
PASS_SOFT_BINS = (1, 2) # Pass unit 정의 기준
```

---

## 4. 모듈 상세

### Module 1: DB 연결 및 데이터 추출

| 함수 | 역할 |
|---|---|
| `get_connection()` | Impala 연결 객체 반환 |
| `fetch_fail_bin_profiles(conn, min_date)` | 과거 fail unit 파라미터 추출 (프로파일 구축용) |
| `fetch_pass_units(conn, target_date)` | 당일 pass unit 파라미터 추출 (유사도 계산용) |

**Pass unit 쿼리 핵심 로직**: `pass_dies` CTE로 `soft_bin IN (1, 2)`인 `die_id`를 먼저 추출한 후, 해당 die의 모든 `test_name` 값을 JOIN하여 가져온다.

```sql
WITH pass_dies AS (
    SELECT DISTINCT die_id
    FROM test_results
    WHERE test_mode = 'xp'
      AND soft_bin IN (1, 2)
      AND CAST(test_datetime AS DATE) = '{target_date}'
)
SELECT t.*
FROM test_results t
INNER JOIN pass_dies p ON t.die_id = p.die_id
WHERE t.test_mode = 'xp'
  AND CAST(t.test_datetime AS DATE) = '{target_date}'
```

### Module 2: Fail Bin 프로파일 구축 및 관리

| 함수 | 역할 |
|---|---|
| `build_bin_profiles(df_fail)` | Fail bin별 feature percentile 계산 |
| `save_profiles(profiles)` | JSON으로 저장 |
| `load_profiles()` | JSON에서 로드 |
| `is_profile_stale(updated_at, refresh_days)` | 갱신 필요 여부 판단 |
| `build_or_refresh_profiles(conn, ...)` | 로드 또는 재계산 자동 결정 |

**프로파일 갱신 흐름**:

```
프로파일 파일 존재?
    NO  → 신규 구축 → 저장
    YES → 갱신일로부터 refresh_days 경과?
              NO  → 기존 프로파일 사용
              YES → 재계산 → 저장
```

### Module 3: Pass Unit 유사도 계산

| 함수 | 역할 |
|---|---|
| `compute_similarity(df_pass, profiles)` | Die별 × Bin별 유사도 계산 |

입력 데이터를 `pivot_table`로 변환하여 행=`die_id`, 열=`test_name` 형태로 만든 후, 각 die의 파라미터 벡터와 각 fail bin 프로파일을 비교한다.

공통 feature가 없는 경우(프로파일에 없는 feature만 보유한 die) 해당 bin 유사도는 `NaN`으로 처리된다.

### Module 4: Wafer / Lot 단위 집계

| 함수 | 역할 |
|---|---|
| `aggregate_similarity(df_sim, df_meta)` | Die 유사도 → Lot Median 집계 |

현재는 Lot 단위 집계만 활성화되어 있다. Wafer 단위 집계는 `wafer_id` 컬럼이 DB에 추가된 후 코드 내 주석을 해제하여 활성화한다.

### Module 5: CUSUM 추세 감지

| 함수 | 역할 |
|---|---|
| `validate_baseline(series, baseline_n)` | Baseline 안정성 검증 및 경고 |
| `cusum_detect(series, k, h, baseline_n)` | 단일 시계열 CUSUM 알람 판정 |
| `detect_all_bins(lot_agg, baseline_n)` | 전체 Fail bin에 CUSUM 일괄 적용 |

CUSUM 기준값(μ, σ)은 시계열 앞쪽 `baseline_n`개 lot으로 추정한다. 따라서 모니터링 시작 초기에는 baseline 구간이 짧아 기준값 신뢰도가 낮을 수 있다.

### Module 6: 알람 출력 및 저장

| 함수 | 역할 |
|---|---|
| `generate_alarms(alarm_df, lot_agg, profiles)` | 알람 상세 정보 생성 |
| `save_results(lot_agg, alarm_df, alerts, target_date)` | CSV 저장 |

**알람 신뢰도**: 프로파일 구축에 사용된 샘플 수가 30개 이상이면 `reliability = 'high'`, 미만이면 `'low'`로 표시한다. 신뢰도가 낮은 알람은 참고 수준으로 해석해야 한다.

**알람 CSV 컬럼**:

| 컬럼 | 설명 |
|---|---|
| `lot_id` | 알람 발생 lot |
| `fail_bin` | 유사해진 fail bin 번호 |
| `similarity` | 해당 lot의 유사도 median (0~1) |
| `n_samples_in_profile` | 프로파일 구축에 사용된 샘플 수 |
| `reliability` | 프로파일 신뢰도 (`high` / `low`) |
| `detected_at` | 감지 일시 |

---

## 5. 실행 방법

### 5.1 의존성 설치

```bash
pip install impyla pandas numpy
```

### 5.2 DB 설정

스크립트 상단 `DB_CONFIG`에 실제 Impala 접속 정보를 입력한다.

```python
DB_CONFIG = {
    'host': 'actual_impala_host',
    'port': 21050,
    'database': 'actual_database',
}
```

### 5.3 실행

```bash
# 당일 날짜로 실행 (기본값)
python bin_similarity_monitor.py

# 날짜 지정
python bin_similarity_monitor.py --date 2025-04-17

# 전체 파라미터 지정
python bin_similarity_monitor.py \
    --date 2025-04-17 \
    --baseline-n 30 \
    --lookback-days 90 \
    --refresh-days 7
```

### 5.4 CLI 인수 요약

| 인수 | 기본값 | 설명 |
|---|---|---|
| `--date` | 오늘 날짜 | 분석 대상 날짜 (YYYY-MM-DD) |
| `--baseline-n` | 30 | CUSUM 기준값 계산에 사용할 최근 lot 수 |
| `--lookback-days` | 90 | 프로파일 구축에 사용할 과거 데이터 기간 (일) |
| `--refresh-days` | 7 | 프로파일 재계산 주기 (일) |

### 5.5 일별 자동 실행 (cron 예시)

```bash
# 매일 오전 6시 실행
0 6 * * * python /path/to/bin_similarity_monitor.py >> /path/to/logs/cron.log 2>&1
```

---

## 6. 로그 해석

로그 레벨별 의미:

| 레벨 | 의미 | 예시 |
|---|---|---|
| `INFO` | 정상 실행 정보 | DB 연결 성공, 데이터 추출 완료 |
| `WARNING` | 주의 필요 사항 | Baseline 불안정, 알람 발생, 샘플 수 부족 |
| `ERROR` | 실행 실패 | DB 연결 실패, 쿼리 오류 |

**로그 예시 (정상)**:
```
[2025-04-17 06:00:01] [INFO] DB 연결 성공
[2025-04-17 06:00:02] [INFO] 기존 프로파일 사용 (갱신일: 2025-04-14)
[2025-04-17 06:00:15] [INFO] Pass unit 데이터 추출 완료: 187,500 rows, 3,750 dies
[2025-04-17 06:02:30] [INFO] 유사도 계산 완료: 3,750 dies × 12 bins
[2025-04-17 06:02:31] [INFO] Lot 집계 완료: 15개 lot
[2025-04-17 06:02:31] [INFO] CUSUM 완료: 0개 lot에서 알람 발생
[2025-04-17 06:02:31] [INFO] 정상: 알람 없음
```

**로그 예시 (알람)**:
```
[2025-04-17 06:02:31] [WARNING] [ALARM] 2건 감지
[2025-04-17 06:02:31] [WARNING]   Lot: LOT_001 | Bin: 5 | 유사도: 0.412 | 신뢰도: high (샘플: 48개)
[2025-04-17 06:02:31] [WARNING]   Lot: LOT_003 | Bin: 5 | 유사도: 0.389 | 신뢰도: low (샘플: 17개)
```

---

## 7. 한계 및 주의사항

**프로파일 초기 불안정**  
Fail bin 샘플이 10~30개 수준일 때 Percentile 추정이 불안정하다. 이 경우 알람의 `reliability = 'low'`로 표시되며, 샘플이 충분히 쌓인 후 재해석을 권장한다.

**Baseline 오염 위험**  
CUSUM baseline 구간(최근 `baseline_n`개 lot) 자체가 이미 drift 중인 경우 기준값이 오염되어 감지 성능이 저하된다. 시스템 도입 초기에는 안정적인 생산 기간의 데이터를 baseline으로 확보하는 것이 중요하다.

**유사도 희석 문제**  
3,000개 feature 전체를 동일 가중치로 계산하면 핵심 파라미터의 신호가 희석될 수 있다. 추후 feature importance 분석을 통해 기여도 높은 feature에 가중치를 부여하는 고도화를 검토할 수 있다.

**Wafer 단위 미구현**  
현재 Lot 단위 집계만 활성화되어 있다. Wafer 단위 분석은 DB에 `wafer_id` 컬럼 추가 후 `aggregate_similarity` 함수 내 주석을 해제하여 활성화한다.

**인과관계 해석 주의**  
유사도 증가는 위험 징후이지, 불량의 직접적 원인이 아니다. 알람 발생 시 해당 fail bin과 연관된 파라미터를 엔지니어가 직접 확인하여 공정/장비 이상 여부를 판단해야 한다.

---

## 8. 향후 개선 방향

- Wafer 단위 집계 활성화 (`wafer_id` 컬럼 추가 후)
- Feature 가중치 적용 (기여도 기반 가중 유사도)
- 알람 발생 시 기여 feature 상위 N개 자동 출력
- 유사도 시계열 시각화 대시보드 연동
- FDR(False Discovery Rate) 제어를 통한 다중 비교 문제 완화
