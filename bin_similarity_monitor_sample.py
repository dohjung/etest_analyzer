"""
Bin Similarity Monitor (샘플 데이터 버전)
==========================================
DB 연결 없이 샘플 CSV 파일로 동일한 알고리듬을 실행합니다.

사전 준비:
    python generate_sample_data.py

실행 방법:
    python bin_similarity_monitor_sample.py --date 2025-04-17
    python bin_similarity_monitor_sample.py --date 2025-04-17 --baseline-n 30
"""

import os
import json
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime

# ============================================================
# 설정
# ============================================================

SAMPLE_DIR      = './data/sample'
FAIL_DATA_PATH  = os.path.join(SAMPLE_DIR, 'fail_data.csv')
PASS_DATA_PATH  = os.path.join(SAMPLE_DIR, 'pass_data.csv')

PROFILE_PATH    = './data/bin_profiles_sample.json'
SIMILARITY_DIR  = './data/similarity'
ALARM_DIR       = './data/alarms'
LOG_DIR         = './logs'

CUSUM_K = 0.5
CUSUM_H = 5.0

PERCENTILE_LOW  = 5
PERCENTILE_HIGH = 95
MIN_SAMPLES     = 10

PASS_SOFT_BINS = (1, 2)


# ============================================================
# 로깅 설정
# ============================================================

def setup_logger(log_dir: str = LOG_DIR) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"monitor_sample_{datetime.now().strftime('%Y%m%d')}.log"
    )
    logger = logging.getLogger('BinMonitorSample')
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
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
# Module 1: 샘플 데이터 로드 (DB 연결 대체)
# ============================================================

def load_fail_data(fail_data_path: str = FAIL_DATA_PATH) -> pd.DataFrame:
    """
    샘플 fail bin 데이터 로드.
    원본의 fetch_fail_bin_profiles() 대체.
    """
    if not os.path.exists(fail_data_path):
        raise FileNotFoundError(
            f"샘플 fail 데이터 없음: {fail_data_path}\n"
            f"먼저 generate_sample_data.py를 실행하세요."
        )
    df = pd.read_csv(fail_data_path)
    logger.info(f"Fail 데이터 로드: {len(df):,} rows, "
                f"{df['die_id'].nunique():,} dies, "
                f"bins={sorted(df['hard_bin'].unique().tolist())}")
    return df


def load_pass_data(target_date: str,
                   pass_data_path: str = PASS_DATA_PATH) -> pd.DataFrame:
    """
    샘플 pass unit 데이터 로드.
    원본의 fetch_pass_units() 대체.

    target_date 와 start_time의 날짜가 일치하는 row만 반환.
    샘플 데이터가 단일 날짜로 생성된 경우 날짜 무관하게 전체 반환.
    """
    if not os.path.exists(pass_data_path):
        raise FileNotFoundError(
            f"샘플 pass 데이터 없음: {pass_data_path}\n"
            f"먼저 generate_sample_data.py를 실행하세요."
        )

    df = pd.read_csv(pass_data_path)
    df['start_time'] = pd.to_datetime(df['start_time'])

    # 날짜 필터링 시도
    df_filtered = df[df['start_time'].dt.date.astype(str) == target_date]

    if df_filtered.empty:
        logger.warning(
            f"{target_date} 에 해당하는 데이터 없음 → "
            f"샘플 전체 데이터 사용 ({df['lot_id'].nunique()} lots)"
        )
        df_filtered = df

    logger.info(f"Pass 데이터 로드 (date={target_date}): "
                f"{len(df_filtered):,} rows, "
                f"{df_filtered['die_id'].nunique():,} dies, "
                f"{df_filtered['lot_id'].nunique()} lots")
    return df_filtered


# ============================================================
# Module 2: Fail Bin 프로파일 구축 및 관리
# (원본과 동일, 파일 경로만 PROFILE_PATH로 변경)
# ============================================================

def build_bin_profiles(df_fail: pd.DataFrame) -> dict:
    """Fail bin별 feature percentile 분포 구축"""
    logger.info("Fail bin 프로파일 구축 시작")

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
    for bin_id in df_pivot['hard_bin'].unique():
        group    = df_pivot[df_pivot['hard_bin'] == bin_id]
        n_samples = len(group)

        if n_samples < MIN_SAMPLES:
            logger.warning(f"Bin {bin_id}: 샘플 수 부족 ({n_samples}개) → 제외")
            continue

        feat_data = group.drop(columns=['die_id', 'hard_bin'])
        profiles[int(bin_id)] = {}
        skipped = 0

        for feat in feat_data.columns:
            values = feat_data[feat].dropna()
            if len(values) < MIN_SAMPLES:
                skipped += 1
                continue
            profiles[int(bin_id)][feat] = {
                'p_low':     float(np.percentile(values, PERCENTILE_LOW)),
                'p_high':    float(np.percentile(values, PERCENTILE_HIGH)),
                'n_samples': int(n_samples),
            }

        logger.info(f"Bin {bin_id}: {len(profiles[int(bin_id)])}개 feature "
                    f"(샘플: {n_samples}, 제외 feature: {skipped})")

    logger.info(f"프로파일 구축 완료: {len(profiles)}개 fail bin")
    return profiles


def save_profiles(profiles: dict, path: str = PROFILE_PATH):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    payload = {
        'updated_at': datetime.now().isoformat(),
        'profiles':   {str(k): v for k, v in profiles.items()},
    }
    with open(path, 'w') as f:
        json.dump(payload, f, indent=2)
    logger.info(f"프로파일 저장: {path}")


def load_profiles(path: str = PROFILE_PATH) -> tuple:
    with open(path, 'r') as f:
        data = json.load(f)
    profiles   = {int(k): v for k, v in data['profiles'].items()}
    updated_at = datetime.fromisoformat(data['updated_at'])
    logger.info(f"프로파일 로드 (갱신일: {updated_at.date()}, {len(profiles)}개 bin)")
    return profiles, updated_at


def is_profile_stale(updated_at: datetime, refresh_days: int) -> bool:
    return (datetime.now() - updated_at).days >= refresh_days


def build_or_refresh_profiles(refresh_days: int = 7) -> dict:
    """
    샘플 fail 데이터로 프로파일 로드 또는 재계산.
    원본의 build_or_refresh_profiles(conn, ...) 대체.
    """
    if os.path.exists(PROFILE_PATH):
        profiles, updated_at = load_profiles()
        if not is_profile_stale(updated_at, refresh_days):
            logger.info(f"기존 프로파일 사용 (갱신일: {updated_at.date()})")
            return profiles
        logger.info(f"{refresh_days}일 경과 → 프로파일 재계산")
    else:
        logger.info("프로파일 없음 → 신규 구축")

    df_fail  = load_fail_data()
    profiles = build_bin_profiles(df_fail)
    save_profiles(profiles)
    return profiles


# ============================================================
# Module 3: Pass Unit 유사도 계산  (원본과 동일)
# ============================================================

def compute_similarity(df_pass: pd.DataFrame,
                       profiles: dict) -> pd.DataFrame:
    """Pass unit별 각 Fail bin과의 유사도 계산 (0~1)"""
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
    n_dies  = len(df_pivot)

    for i, (die_id, unit_values) in enumerate(df_pivot.iterrows()):
        row = {'die_id': die_id}
        for bin_id, bin_profile in profiles.items():
            common = [
                f for f in bin_profile
                if f in unit_values.index and pd.notna(unit_values[f])
            ]
            if not common:
                row[f'sim_bin_{bin_id}'] = np.nan
                continue
            in_range = sum(
                1 for f in common
                if bin_profile[f]['p_low'] <= unit_values[f] <= bin_profile[f]['p_high']
            )
            row[f'sim_bin_{bin_id}'] = in_range / len(common)
        results.append(row)

        if (i + 1) % 1000 == 0:
            logger.debug(f"유사도 계산: {i+1:,}/{n_dies:,} dies")

    df_sim = pd.DataFrame(results)
    logger.info(f"유사도 계산 완료: {len(df_sim):,} dies × {len(profiles)} bins")
    return df_sim


# ============================================================
# Module 4: Wafer / Lot 단위 집계  (원본과 동일)
# ============================================================

def aggregate_similarity(df_sim: pd.DataFrame,
                         df_meta: pd.DataFrame) -> dict:
    """Lot 단위 유사도 median 집계"""
    logger.info("Lot 단위 집계 시작")
    df      = df_sim.merge(df_meta[['die_id', 'lot_id']], on='die_id', how='left')
    sim_cols = [c for c in df.columns if c.startswith('sim_bin_')]
    lot_agg  = df.groupby('lot_id')[sim_cols].median()
    logger.info(f"Lot 집계 완료: {len(lot_agg)}개 lot")
    return {'lot': lot_agg}


# ============================================================
# Module 5: CUSUM 추세 감지  (원본과 동일)
# ============================================================

def validate_baseline(series: pd.Series,
                      baseline_n: int,
                      std_threshold: float = 0.1) -> bool:
    baseline = series.dropna().values[:baseline_n]
    if len(baseline) == 0:
        return False
    std = np.std(baseline)
    if std > std_threshold:
        logger.warning(f"Baseline 불안정 (std={std:.3f} > {std_threshold})")
        return False
    return True


def cusum_detect(series: pd.Series,
                 k: float = CUSUM_K,
                 h: float = CUSUM_H,
                 baseline_n: int = 30) -> pd.Series:
    """CUSUM drift 감지"""
    values       = series.values
    n            = len(values)
    baseline_end = min(baseline_n, n)
    baseline     = values[:baseline_end]

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
        z     = (val - mu) / sigma
        S_pos = max(0.0, S_pos + z - k)
        S_neg = max(0.0, S_neg - z - k)
        alarms.append(bool(S_pos > h or S_neg > h))

    return pd.Series(alarms, index=series.index)


def detect_all_bins(lot_agg: pd.DataFrame,
                    baseline_n: int = 30) -> pd.DataFrame:
    """모든 fail bin에 대해 CUSUM 적용"""
    sim_cols = [c for c in lot_agg.columns if c.startswith('sim_bin_')]
    alarm_df = pd.DataFrame(index=lot_agg.index)

    for col in sim_cols:
        validate_baseline(lot_agg[col], baseline_n)
        alarm_df[f'alarm_{col}'] = cusum_detect(lot_agg[col], baseline_n=baseline_n)

    n_alarms = alarm_df.any(axis=1).sum()
    logger.info(f"CUSUM 완료: {n_alarms}개 lot에서 알람 발생")
    return alarm_df


# ============================================================
# Module 6: 알람 출력 및 저장  (원본과 동일)
# ============================================================

def generate_alarms(alarm_df: pd.DataFrame,
                    lot_agg: pd.DataFrame,
                    profiles: dict) -> list:
    alarm_cols = [c for c in alarm_df.columns if c.startswith('alarm_')]
    alerts     = []

    for lot_id, row in alarm_df.iterrows():
        for alarm_col in alarm_cols:
            if not row[alarm_col]:
                continue
            bin_id     = int(alarm_col.replace('alarm_sim_bin_', ''))
            sim_col    = f'sim_bin_{bin_id}'
            similarity = lot_agg.loc[lot_id, sim_col]

            n_samples = 0
            if bin_id in profiles and profiles[bin_id]:
                n_samples = list(profiles[bin_id].values())[0]['n_samples']
            reliability = 'high' if n_samples >= 30 else 'low'

            alerts.append({
                'lot_id':               lot_id,
                'fail_bin':             bin_id,
                'similarity':           round(float(similarity), 4),
                'n_samples_in_profile': n_samples,
                'reliability':          reliability,
                'detected_at':          datetime.now().isoformat(),
            })

    return alerts


def save_results(lot_agg: pd.DataFrame,
                 alarm_df: pd.DataFrame,
                 alerts: list,
                 target_date: str):
    date_str = target_date.replace('-', '')
    os.makedirs(SIMILARITY_DIR, exist_ok=True)
    os.makedirs(ALARM_DIR,      exist_ok=True)

    sim_path = os.path.join(SIMILARITY_DIR, f'similarity_{date_str}_sample.csv')
    lot_agg.to_csv(sim_path)
    logger.info(f"유사도 저장: {sim_path}")

    alarm_path = os.path.join(ALARM_DIR, f'alarms_{date_str}_sample.csv')
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
        refresh_days: int = 7):
    """
    샘플 데이터로 전체 파이프라인 실행.

    Args:
        target_date:  분석 대상 날짜 (YYYY-MM-DD)
        baseline_n:   CUSUM 기준값 계산에 사용할 최근 lot 수
        refresh_days: 프로파일 재계산 주기 (일)
    """
    logger.info(f"{'='*50}")
    logger.info(f"Bin Similarity Monitor (샘플) 실행: {target_date}")
    logger.info(f"{'='*50}")

    # Step 1: 프로파일 로드 또는 재계산
    profiles = build_or_refresh_profiles(refresh_days=refresh_days)

    # Step 2: Pass unit 데이터 로드
    df_pass = load_pass_data(target_date)
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
    parser = argparse.ArgumentParser(description='Bin Similarity Monitor (샘플 데이터)')
    parser.add_argument('--date',         type=str, default='2025-04-17',
                        help='분석 대상 날짜 (YYYY-MM-DD, 기본: 2025-04-17)')
    parser.add_argument('--baseline-n',   type=int, default=30,
                        help='CUSUM baseline lot 수 (기본: 30)')
    parser.add_argument('--refresh-days', type=int, default=7,
                        help='프로파일 재계산 주기 (기본: 7일)')
    args = parser.parse_args()

    run(
        target_date=args.date,
        baseline_n=args.baseline_n,
        refresh_days=args.refresh_days,
    )
