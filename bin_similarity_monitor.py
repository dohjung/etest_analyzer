"""
Bin Similarity Monitor
======================
Pass unit이 특정 Fail bin과 유사해지는 추세를 감지하는 모니터링 시스템.

실행 방법:
    python bin_similarity_monitor.py --date 2025-04-17
    python bin_similarity_monitor.py --date 2025-04-17 --baseline-n 30 --refresh-days 7
"""

import os
import json
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime
# from impyla.dbapi import connect

# ============================================================
# 설정
# ============================================================

DB_CONFIG = {
    'host': 'your_impala_host',
    'port': 21050,
    'database': 'your_database',
}

# 저장 경로
PROFILE_PATH    = './data/bin_profiles.json'
SIMILARITY_DIR  = './data/similarity'
ALARM_DIR       = './data/alarms'
LOG_DIR         = './logs'

# CUSUM 파라미터
CUSUM_K = 0.5   # 허용 slack
CUSUM_H = 5.0   # 알람 threshold

# Percentile 범위
PERCENTILE_LOW  = 5
PERCENTILE_HIGH = 95

# Fail bin 프로파일 구축 최소 샘플 수
MIN_SAMPLES = 10

# Pass unit 정의: soft_bin IN (1, 2)
PASS_SOFT_BINS = (1, 2)


# ============================================================
# 로깅 설정
# ============================================================

def setup_logger(log_dir: str = LOG_DIR) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"monitor_{datetime.now().strftime('%Y%m%d')}.log"
    )
    logger = logging.getLogger('BinMonitor')
    logger.setLevel(logging.DEBUG)

    # 파일 핸들러
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)

    # 콘솔 핸들러
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = setup_logger()


# ============================================================
# Module 1: DB 연결 및 데이터 추출
# ============================================================

def get_connection():
    """Impala DB 연결"""
    try:
        conn = connect(**DB_CONFIG)
        logger.info("DB 연결 성공")
        return conn
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        raise


def fetch_fail_bin_profiles(conn, min_date: str) -> pd.DataFrame:
    """
    1차 테스트(xp) fail unit의 파라미터 값 추출.
    Fail bin 프로파일 구축용.

    Args:
        min_date: 데이터 시작 날짜 (예: '2025-01-01')
    """
    query = f"""
    SELECT
        lot_id,
        die_id,
        start_time,
        test_txt,
        pin,
        test_result,
        hard_bin,
        soft_bin
    FROM test_results
    WHERE test_mode = 'xp'
        AND soft_bin NOT IN {PASS_SOFT_BINS}
        AND test_datetime >= '{min_date}'
    """
    logger.info(f"Fail bin 데이터 추출 시작 (from {min_date})")
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Fail bin 데이터 추출 완료: {len(df):,} rows, "
                    f"{df['die_id'].nunique():,} dies")
        return df
    except Exception as e:
        logger.error(f"Fail bin 데이터 추출 실패: {e}")
        raise


def fetch_pass_units(conn, target_date: str) -> pd.DataFrame:
    """
    특정 날짜의 1차 테스트(xp) pass unit 파라미터 값 추출.
    soft_bin IN (1, 2)인 die만 pass로 정의.

    Args:
        target_date: 분석 대상 날짜 (예: '2025-04-17')
    """
    query = f"""
    WITH pass_dies AS (
        SELECT DISTINCT die_id
        FROM test_results
        WHERE test_mode = 'xp'
            AND soft_bin IN {PASS_SOFT_BINS}
            AND CAST(test_datetime AS DATE) = '{target_date}'
    )
    SELECT
        t.lot_id,
        t.die_id,
        t.start_time,
        t.test_txt,
        t.pin,
        t.test_result,
        t.soft_bin,
        t.hard_bin
    FROM test_results t
    INNER JOIN pass_dies p ON t.die_id = p.die_id
    WHERE t.test_mode = 'xp'
        AND CAST(t.test_datetime AS DATE) = '{target_date}'
    """
    logger.info(f"Pass unit 데이터 추출 시작 (date: {target_date})")
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Pass unit 데이터 추출 완료: {len(df):,} rows, "
                    f"{df['die_id'].nunique():,} dies")
        return df
    except Exception as e:
        logger.error(f"Pass unit 데이터 추출 실패: {e}")
        raise


# ============================================================
# Module 2: Fail Bin 프로파일 구축 및 관리
# ============================================================

def build_bin_profiles(df_fail: pd.DataFrame) -> dict:
    """
    Fail bin별 feature의 percentile 분포 구축.

    Returns:
        {
            hard_bin_id (int): {
                feature_name (str): {
                    'p_low': float,
                    'p_high': float,
                    'n_samples': int
                }
            }
        }
    """
    logger.info("Fail bin 프로파일 구축 시작")

    # die 단위 pivot: 행=die_id, 열=test_txt
    try:
        df_pivot = df_fail.pivot_table(
            index=['die_id', 'hard_bin'],
            columns='test_txt',
            values='test_result',
            aggfunc='first'
        ).reset_index()
    except Exception as e:
        logger.error(f"Pivot 실패: {e}")
        raise

    profiles = {}
    bin_ids = df_pivot['hard_bin'].unique()

    for bin_id in bin_ids:
        group = df_pivot[df_pivot['hard_bin'] == bin_id]
        n_samples = len(group)

        if n_samples < MIN_SAMPLES:
            logger.warning(f"Bin {bin_id}: 샘플 수 부족 ({n_samples}개) → 프로파일 제외")
            continue

        feature_data = group.drop(columns=['die_id', 'hard_bin'])
        profiles[int(bin_id)] = {}

        skipped = 0
        for feature in feature_data.columns:
            values = feature_data[feature].dropna()
            if len(values) < MIN_SAMPLES:
                skipped += 1
                continue
            profiles[int(bin_id)][feature] = {
                'p_low':     float(np.percentile(values, PERCENTILE_LOW)),
                'p_high':    float(np.percentile(values, PERCENTILE_HIGH)),
                'n_samples': int(n_samples)
            }

        logger.info(f"Bin {bin_id}: {len(profiles[int(bin_id)])}개 feature 프로파일 구축 "
                    f"(샘플 수: {n_samples}, 제외 feature: {skipped}개)")

    logger.info(f"프로파일 구축 완료: {len(profiles)}개 fail bin")
    return profiles


def save_profiles(profiles: dict, path: str = PROFILE_PATH):
    """프로파일을 JSON으로 저장"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        'updated_at': datetime.now().isoformat(),
        'profiles': {str(k): v for k, v in profiles.items()}
    }
    with open(path, 'w') as f:
        json.dump(payload, f, indent=2)
    logger.info(f"프로파일 저장 완료: {path}")


def load_profiles(path: str = PROFILE_PATH) -> tuple:
    """
    저장된 프로파일 로드.

    Returns:
        (profiles dict, updated_at datetime)
    """
    with open(path, 'r') as f:
        data = json.load(f)
    profiles = {int(k): v for k, v in data['profiles'].items()}
    updated_at = datetime.fromisoformat(data['updated_at'])
    logger.info(f"프로파일 로드 완료 (갱신일: {updated_at.date()}, "
                f"{len(profiles)}개 bin)")
    return profiles, updated_at


def is_profile_stale(updated_at: datetime, refresh_days: int) -> bool:
    """프로파일 갱신 필요 여부 확인"""
    elapsed = (datetime.now() - updated_at).days
    return elapsed >= refresh_days


def build_or_refresh_profiles(conn,
                               lookback_days: int = 90,
                               refresh_days: int = 7) -> dict:
    """
    프로파일 로드 또는 재계산.

    Args:
        lookback_days: 프로파일 구축에 사용할 과거 데이터 기간 (일)
        refresh_days:  재계산 주기 (일)
    """
    if os.path.exists(PROFILE_PATH):
        profiles, updated_at = load_profiles()
        if not is_profile_stale(updated_at, refresh_days):
            logger.info(f"기존 프로파일 사용 (갱신일: {updated_at.date()})")
            return profiles
        logger.info(f"{refresh_days}일 경과 → 프로파일 재계산 시작")
    else:
        logger.info("프로파일 없음 → 신규 구축 시작")

    from_date = (
        pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
    ).strftime('%Y-%m-%d')

    df_fail = fetch_fail_bin_profiles(conn, min_date=from_date)
    profiles = build_bin_profiles(df_fail)
    save_profiles(profiles)
    return profiles


# ============================================================
# Module 3: Pass Unit 유사도 계산
# ============================================================

def compute_similarity(df_pass: pd.DataFrame,
                       profiles: dict) -> pd.DataFrame:
    """
    Pass unit별 각 Fail bin과의 유사도 계산.
    유사도 = fail bin percentile 범위 내에 있는 feature 비율 (0~1).

    Returns:
        DataFrame: 행=die_id, 열=sim_bin_{bin_id}
    """
    logger.info("유사도 계산 시작")

    try:
        df_pivot = df_pass.pivot_table(
            index='die_id',
            columns='test_txt',
            values='test_result',
            aggfunc='first'
        )
    except Exception as e:
        logger.error(f"Pass unit pivot 실패: {e}")
        raise

    results = []
    n_dies = len(df_pivot)

    for i, (die_id, unit_values) in enumerate(df_pivot.iterrows()):
        row = {'die_id': die_id}

        for bin_id, bin_profile in profiles.items():
            common_features = [
                f for f in bin_profile
                if f in unit_values.index and pd.notna(unit_values[f])
            ]
            if not common_features:
                row[f'sim_bin_{bin_id}'] = np.nan
                continue

            in_range = sum(
                1 for f in common_features
                if bin_profile[f]['p_low'] <= unit_values[f] <= bin_profile[f]['p_high']
            )
            row[f'sim_bin_{bin_id}'] = in_range / len(common_features)

        results.append(row)

        if (i + 1) % 1000 == 0:
            logger.debug(f"유사도 계산 진행: {i+1:,}/{n_dies:,} dies")

    df_sim = pd.DataFrame(results)
    logger.info(f"유사도 계산 완료: {len(df_sim):,} dies × {len(profiles)} bins")
    return df_sim


# ============================================================
# Module 4: Wafer / Lot 단위 집계
# ============================================================

def aggregate_similarity(df_sim: pd.DataFrame,
                         df_meta: pd.DataFrame) -> dict:
    """
    Lot 단위로 유사도 median 집계.
    Wafer 단위는 wafer_id 컬럼 추가 후 활성화.

    Args:
        df_sim:  die_id + sim_bin_* 컬럼
        df_meta: die_id, lot_id 포함

    Returns:
        {'lot': DataFrame (index=lot_id, columns=sim_bin_*)}
    """
    logger.info("Lot/Wafer 단위 집계 시작")

    df = df_sim.merge(df_meta[['die_id', 'lot_id']], on='die_id', how='left')
    sim_cols = [c for c in df.columns if c.startswith('sim_bin_')]

    # Lot 단위 집계
    lot_agg = df.groupby('lot_id')[sim_cols].median()
    logger.info(f"Lot 집계 완료: {len(lot_agg)}개 lot")

    # Wafer 단위 집계 (wafer_id 추가 후 활성화)
    # if 'wafer_id' in df.columns:
    #     wafer_agg = df.groupby('wafer_id')[sim_cols].median()
    #     logger.info(f"Wafer 집계 완료: {len(wafer_agg)}개 wafer")

    return {'lot': lot_agg}


# ============================================================
# Module 5: CUSUM 추세 감지
# ============================================================

def validate_baseline(series: pd.Series,
                      baseline_n: int,
                      std_threshold: float = 0.1) -> bool:
    """
    Baseline 구간 안정성 확인.
    표준편차가 threshold 초과 시 경고.
    """
    baseline = series.dropna().values[:baseline_n]
    if len(baseline) == 0:
        return False
    std = np.std(baseline)
    if std > std_threshold:
        logger.warning(f"Baseline 불안정 (std={std:.3f} > {std_threshold}) "
                       f"→ 기준값 신뢰도 낮음")
        return False
    return True


def cusum_detect(series: pd.Series,
                 k: float = CUSUM_K,
                 h: float = CUSUM_H,
                 baseline_n: int = 30) -> pd.Series:
    """
    CUSUM drift 감지.

    Args:
        series:     시계열 유사도 (lot 순서)
        k:          허용 slack (통상 0.5)
        h:          알람 threshold (통상 4~5)
        baseline_n: 기준값 계산에 사용할 최근 lot 수

    Returns:
        Boolean Series (True = 알람)
    """
    values = series.values
    n = len(values)
    baseline_end = min(baseline_n, n)
    baseline = values[:baseline_end]

    # NaN 제거 후 기준값 계산
    baseline_clean = baseline[~np.isnan(baseline)]
    if len(baseline_clean) == 0:
        return pd.Series([False] * n, index=series.index)

    mu    = np.mean(baseline_clean)
    sigma = np.std(baseline_clean)

    if sigma == 0:
        logger.warning("Baseline sigma=0 → CUSUM 계산 불가")
        return pd.Series([False] * n, index=series.index)

    S_pos, S_neg = 0.0, 0.0
    alarms = []
    for val in values:
        if np.isnan(val):
            alarms.append(False)
            continue
        z = (val - mu) / sigma
        S_pos = max(0.0, S_pos + z - k)
        S_neg = max(0.0, S_neg - z - k)
        alarms.append(bool(S_pos > h or S_neg > h))

    return pd.Series(alarms, index=series.index)


def detect_all_bins(lot_agg: pd.DataFrame,
                    baseline_n: int = 30) -> pd.DataFrame:
    """
    모든 fail bin에 대해 CUSUM 적용.

    Args:
        lot_agg:    lot 단위 유사도 DataFrame
        baseline_n: 기준값 계산에 사용할 최근 lot 수
    """
    sim_cols = [c for c in lot_agg.columns if c.startswith('sim_bin_')]
    alarm_df = pd.DataFrame(index=lot_agg.index)

    for col in sim_cols:
        validate_baseline(lot_agg[col], baseline_n)
        alarm_df[f'alarm_{col}'] = cusum_detect(
            lot_agg[col],
            baseline_n=baseline_n
        )

    n_alarms = alarm_df.any(axis=1).sum()
    logger.info(f"CUSUM 완료: {n_alarms}개 lot에서 알람 발생")
    return alarm_df


# ============================================================
# Module 6: 알람 출력 및 저장
# ============================================================

def generate_alarms(alarm_df: pd.DataFrame,
                    lot_agg: pd.DataFrame,
                    profiles: dict) -> list:
    """
    알람 발생 lot 정보 생성.

    Returns:
        list of dict: 알람 상세 정보
    """
    alarm_cols = [c for c in alarm_df.columns if c.startswith('alarm_')]
    alerts = []

    for lot_id, row in alarm_df.iterrows():
        for alarm_col in alarm_cols:
            if not row[alarm_col]:
                continue

            bin_id  = int(alarm_col.replace('alarm_sim_bin_', ''))
            sim_col = f'sim_bin_{bin_id}'
            similarity = lot_agg.loc[lot_id, sim_col]

            # 프로파일 신뢰도 판단
            n_samples = 0
            if bin_id in profiles and profiles[bin_id]:
                n_samples = list(profiles[bin_id].values())[0]['n_samples']
            reliability = 'high' if n_samples >= 30 else 'low'

            alerts.append({
                'lot_id':                lot_id,
                'fail_bin':              bin_id,
                'similarity':            round(float(similarity), 4),
                'n_samples_in_profile':  n_samples,
                'reliability':           reliability,
                'detected_at':           datetime.now().isoformat()
            })

    return alerts


def save_results(lot_agg: pd.DataFrame,
                 alarm_df: pd.DataFrame,
                 alerts: list,
                 target_date: str):
    """
    유사도, 알람 결과를 CSV로 저장.

    저장 파일:
        similarity/similarity_YYYYMMDD.csv
        alarms/alarms_YYYYMMDD.csv
    """
    date_str = target_date.replace('-', '')
    os.makedirs(SIMILARITY_DIR, exist_ok=True)
    os.makedirs(ALARM_DIR, exist_ok=True)

    # 유사도 저장
    sim_path = os.path.join(SIMILARITY_DIR, f'similarity_{date_str}.csv')
    lot_agg.to_csv(sim_path)
    logger.info(f"유사도 저장: {sim_path}")

    # 알람 저장
    alarm_path = os.path.join(ALARM_DIR, f'alarms_{date_str}.csv')
    if alerts:
        pd.DataFrame(alerts).to_csv(alarm_path, index=False)
        logger.warning(f"알람 저장: {alarm_path} ({len(alerts)}건)")
    else:
        logger.info("알람 없음")


# ============================================================
# 메인 실행
# ============================================================

def run(target_date: str,
        baseline_n: int = 30,
        lookback_days: int = 90,
        refresh_days: int = 7):
    """
    전체 파이프라인 실행.

    Args:
        target_date:   분석 대상 날짜 (YYYY-MM-DD)
        baseline_n:    CUSUM 기준값 계산에 사용할 최근 lot 수
        lookback_days: 프로파일 구축에 사용할 과거 데이터 기간 (일)
        refresh_days:  프로파일 재계산 주기 (일)
    """
    logger.info(f"{'='*50}")
    logger.info(f"Bin Similarity Monitor 실행: {target_date}")
    logger.info(f"{'='*50}")

    conn = get_connection()

    # Step 1: 프로파일 로드 또는 재계산
    profiles = build_or_refresh_profiles(
        conn,
        lookback_days=lookback_days,
        refresh_days=refresh_days
    )

    # Step 2: Pass unit 데이터 추출
    df_pass = fetch_pass_units(conn, target_date=target_date)
    if df_pass.empty:
        logger.warning(f"{target_date} 데이터 없음 → 종료")
        return

    # Step 3: 유사도 계산
    df_meta = df_pass[['die_id', 'lot_id']].drop_duplicates()
    df_sim  = compute_similarity(df_pass, profiles)

    # Step 4: Lot 단위 집계
    agg = aggregate_similarity(df_sim, df_meta)

    # Step 5: CUSUM 추세 감지
    alarm_df = detect_all_bins(agg['lot'], baseline_n=baseline_n)

    # Step 6: 알람 생성 및 저장
    alerts = generate_alarms(alarm_df, agg['lot'], profiles)
    save_results(agg['lot'], alarm_df, alerts, target_date)

    # 콘솔 출력
    if alerts:
        logger.warning(f"[ALARM] {len(alerts)}건 감지")
        for a in alerts:
            logger.warning(
                f"  Lot: {a['lot_id']} | Bin: {a['fail_bin']} | "
                f"유사도: {a['similarity']:.3f} | "
                f"신뢰도: {a['reliability']} (샘플: {a['n_samples_in_profile']}개)"
            )
    else:
        logger.info("정상: 알람 없음")

    logger.info("실행 완료")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bin Similarity Monitor')
    parser.add_argument('--date',          type=str, default=datetime.now().strftime('%Y-%m-%d'),
                        help='분석 대상 날짜 (YYYY-MM-DD)')
    parser.add_argument('--baseline-n',    type=int, default=30,
                        help='CUSUM baseline lot 수 (기본: 30)')
    parser.add_argument('--lookback-days', type=int, default=90,
                        help='프로파일 구축 기간 (기본: 90일)')
    parser.add_argument('--refresh-days',  type=int, default=7,
                        help='프로파일 재계산 주기 (기본: 7일)')
    args = parser.parse_args()

    run(
        target_date=args.date,
        baseline_n=args.baseline_n,
        lookback_days=args.lookback_days,
        refresh_days=args.refresh_days
    )
