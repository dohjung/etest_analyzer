"""
Feature Filter
==============
pass unit과 fail unit의 분포 차이를 기반으로
test_diff / test_sim을 분류하는 모듈.

실행 방법:
    python feature_filter.py
    python feature_filter.py --lookback-days 90 --refresh-days 7 --oa-threshold 0.1 --cohens-d-threshold 0.8
"""

import os
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime
from scipy.stats import gaussian_kde

from bin_similarity_monitor import DB_CONFIG, get_connection, LOG_DIR, PASS_SOFT_BINS

# ============================================================
# 설정
# ============================================================

# OA 계산에 필요한 최소 fail unit 샘플 수
OA_MIN_SAMPLES = 50

FILTER_DIR = './data/feature_filter'


# ============================================================
# 로깅 설정
# ============================================================

def setup_logger(log_dir: str = LOG_DIR) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"feature_filter_{datetime.now().strftime('%Y%m%d')}.log"
    )
    logger = logging.getLogger('FeatureFilter')
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

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
# Module 1: 데이터 추출
# ============================================================

def fetch_classification_data(conn, min_date: str) -> pd.DataFrame:
    """
    분류에 사용할 pass/fail unit 데이터 추출.
    test_mode = 'xp'만 사용.
    """
    query = f"""
    SELECT
        die_id,
        test_name,
        test_result,
        soft_bin
    FROM test_results
    WHERE test_mode = 'xp'
        AND test_datetime >= '{min_date}'
        AND test_result IS NOT NULL
    """
    logger.info(f"분류 데이터 추출 시작 (from {min_date})")
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"데이터 추출 완료: {len(df):,} rows")
        return df
    except Exception as e:
        logger.error(f"데이터 추출 실패: {e}")
        raise


# ============================================================
# Module 2: 분류 통계 계산
# ============================================================

def compute_overlap_area(pass_vals: np.ndarray,
                         fail_vals: np.ndarray,
                         n_points: int = 1000) -> float:
    """
    KDE 기반 두 분포의 Overlap Area 계산 (0~1).
    scipy.stats.gaussian_kde + numpy.trapz 사용.
    """
    combined = np.concatenate([pass_vals, fail_vals])
    x_min, x_max = combined.min(), combined.max()
    if x_min == x_max:
        return 1.0

    x = np.linspace(x_min, x_max, n_points)

    kde_pass = gaussian_kde(pass_vals)
    kde_fail = gaussian_kde(fail_vals)

    oa = float(np.trapz(np.minimum(kde_pass(x), kde_fail(x)), x))
    return float(np.clip(oa, 0.0, 1.0))


def compute_cohens_d(pass_vals: np.ndarray, fail_vals: np.ndarray) -> float:
    """
    Cohen's d 효과 크기 계산.
    d = (μ_fail - μ_pass) / pooled_std
    """
    n1, n2 = len(pass_vals), len(fail_vals)
    if n1 < 2 or n2 < 2:
        return 0.0

    s1 = np.std(pass_vals, ddof=1)
    s2 = np.std(fail_vals, ddof=1)
    pooled_std = np.sqrt(((n1 - 1) * s1**2 + (n2 - 1) * s2**2) / (n1 + n2 - 2))

    if pooled_std == 0:
        return 0.0

    return float((np.mean(fail_vals) - np.mean(pass_vals)) / pooled_std)


def classify_test_names(df: pd.DataFrame,
                        oa_threshold: float = 0.1,
                        cohens_d_threshold: float = 0.8) -> pd.DataFrame:
    """
    각 test_name에 대해 pass/fail 분포를 비교하여 test_diff / test_sim 분류.

    분류 로직:
      - 케이스 A (fail n >= OA_MIN_SAMPLES): OA + Cohen's d 조합
      - 케이스 B (10 <= fail n < OA_MIN_SAMPLES): Cohen's d만 사용
      - 케이스 C (fail n < 10 또는 pass n < 30): 분류 제외 (WARNING 로그)
    """
    logger.info("test_name 분류 시작")

    is_pass = df['soft_bin'].isin(PASS_SOFT_BINS)
    df_pass_all = df[is_pass]
    df_fail_all = df[~is_pass]

    test_names = df['test_name'].unique()
    logger.info(f"전체 test_name: {len(test_names):,}개")

    records = []
    excluded = 0
    classified_at = datetime.now().isoformat()

    for test_name in test_names:
        pass_vals = (
            df_pass_all.loc[df_pass_all['test_name'] == test_name, 'test_result']
            .dropna().values
        )
        fail_vals = (
            df_fail_all.loc[df_fail_all['test_name'] == test_name, 'test_result']
            .dropna().values
        )

        n_pass = len(pass_vals)
        n_fail = len(fail_vals)

        # 케이스 C: 샘플 수 미달 → 제외
        if n_pass < 30 or n_fail < 10:
            logger.warning(
                f"[EXCLUDE] {test_name}: n_pass={n_pass}, n_fail={n_fail} → 샘플 수 부족"
            )
            excluded += 1
            continue

        cohens_d = compute_cohens_d(pass_vals, fail_vals)
        abs_d = abs(cohens_d)

        if n_fail >= OA_MIN_SAMPLES:
            # 케이스 A: OA + Cohen's d 조합
            oa = compute_overlap_area(pass_vals, fail_vals)
            is_diff = (oa < oa_threshold) and (abs_d >= cohens_d_threshold)
            method = 'oa+cohens'
        else:
            # 케이스 B: Cohen's d만 사용
            oa = np.nan
            is_diff = abs_d >= cohens_d_threshold
            method = 'cohens_only'

        label = f"test_diff_{method}" if is_diff else f"test_sim_{method}"

        records.append({
            'test_name':     test_name,
            'overlap_area':  oa,
            'cohens_d':      cohens_d,
            'n_pass':        n_pass,
            'n_fail':        n_fail,
            'label':         label,
            'classified_at': classified_at,
        })

    n_diff = sum(1 for r in records if r['label'].startswith('test_diff'))
    logger.info(
        f"분류 완료: {len(records)}개 처리, {excluded}개 제외 "
        f"(test_diff={n_diff}, test_sim={len(records) - n_diff})"
    )
    return pd.DataFrame(records)


# ============================================================
# Module 3: 저장 및 로드
# ============================================================

def _today_str() -> str:
    return datetime.now().strftime('%Y%m%d')


def _filter_path() -> str:
    return os.path.join(FILTER_DIR, f'feature_filter_{_today_str()}.csv')


def _diff_path() -> str:
    return os.path.join(FILTER_DIR, f'test_diff_{_today_str()}.csv')


def save_classification(df_result: pd.DataFrame):
    """전체 분류 결과 및 test_diff 목록을 CSV로 저장."""
    os.makedirs(FILTER_DIR, exist_ok=True)

    filter_path = _filter_path()
    df_result.to_csv(filter_path, index=False)
    logger.info(f"분류 결과 저장: {filter_path}")

    diff_path = _diff_path()
    df_diff = df_result[df_result['label'].str.startswith('test_diff')][
        ['test_name', 'overlap_area', 'cohens_d']
    ]
    df_diff.to_csv(diff_path, index=False)
    logger.info(f"test_diff 목록 저장: {diff_path} ({len(df_diff)}개)")


def load_test_diff(path: str) -> set:
    """
    저장된 test_diff 목록을 로드하여 set으로 반환.
    파일이 없으면 None을 반환하고 WARNING 로그 출력.
    """
    if not os.path.exists(path):
        logger.warning(f"test_diff 파일 없음: {path}")
        return None
    try:
        df = pd.read_csv(path)
        result = set(df['test_name'].tolist())
        logger.info(f"test_diff 로드: {len(result)}개 test_name ({path})")
        return result
    except Exception as e:
        logger.error(f"test_diff 파일 로드 실패: {e}")
        return None


# ============================================================
# Module 4: 자동 분류 주기 관리
# ============================================================

def _find_latest_filter_file() -> tuple:
    """
    가장 최근 feature_filter_YYYYMMDD.csv 파일 경로와 날짜 반환.
    없으면 (None, None).
    """
    if not os.path.isdir(FILTER_DIR):
        return None, None

    files = [
        f for f in os.listdir(FILTER_DIR)
        if f.startswith('feature_filter_') and f.endswith('.csv')
    ]
    if not files:
        return None, None

    def _parse_dt(fname):
        try:
            return datetime.strptime(fname, 'feature_filter_%Y%m%d.csv')
        except ValueError:
            return datetime.min

    latest = max(files, key=_parse_dt)
    return os.path.join(FILTER_DIR, latest), _parse_dt(latest)


def _find_latest_diff_file() -> str:
    """가장 최근 test_diff_YYYYMMDD.csv 경로 반환. 없으면 None."""
    if not os.path.isdir(FILTER_DIR):
        return None

    files = [
        f for f in os.listdir(FILTER_DIR)
        if f.startswith('test_diff_') and f.endswith('.csv')
    ]
    if not files:
        return None

    def _parse_dt(fname):
        try:
            return datetime.strptime(fname, 'test_diff_%Y%m%d.csv')
        except ValueError:
            return datetime.min

    return os.path.join(FILTER_DIR, max(files, key=_parse_dt))


def classify_or_load(conn,
                     lookback_days: int = 90,
                     refresh_days: int = 7,
                     oa_threshold: float = 0.1,
                     cohens_d_threshold: float = 0.8) -> str:
    """
    분류 결과 파일 로드 또는 재분류 수행.
    bin_similarity_monitor.py의 build_or_refresh_profiles와 동일한 패턴.

    Returns:
        최신 test_diff CSV 경로 (문자열)
    """
    filter_path, filter_dt = _find_latest_filter_file()

    if filter_path is not None:
        elapsed = (datetime.now() - filter_dt).days
        if elapsed < refresh_days:
            logger.info(f"기존 분류 결과 사용 (생성일: {filter_dt.date()})")
            return _find_latest_diff_file()
        logger.info(f"{refresh_days}일 경과 → 재분류 시작")
    else:
        logger.info("분류 결과 없음 → 신규 분류 시작")

    from_date = (
        pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
    ).strftime('%Y-%m-%d')

    df = fetch_classification_data(conn, min_date=from_date)
    df_result = classify_test_names(df, oa_threshold, cohens_d_threshold)
    save_classification(df_result)

    return _diff_path()


# ============================================================
# 메인 실행
# ============================================================

def run(lookback_days: int = 90,
        refresh_days: int = 7,
        oa_threshold: float = 0.1,
        cohens_d_threshold: float = 0.8):
    logger.info('=' * 50)
    logger.info('Feature Filter 실행')
    logger.info('=' * 50)

    conn = get_connection()
    diff_path = classify_or_load(
        conn,
        lookback_days=lookback_days,
        refresh_days=refresh_days,
        oa_threshold=oa_threshold,
        cohens_d_threshold=cohens_d_threshold,
    )

    test_diff_set = load_test_diff(diff_path) if diff_path else None
    if test_diff_set is not None:
        logger.info(f"test_diff 목록 준비 완료: {len(test_diff_set)}개")
    else:
        logger.warning("test_diff 목록 준비 실패")

    logger.info("실행 완료")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Feature Filter: test_diff / test_sim 분류'
    )
    parser.add_argument('--lookback-days',      type=int,   default=90,
                        help='분류에 사용할 과거 데이터 기간 (기본: 90)')
    parser.add_argument('--refresh-days',       type=int,   default=7,
                        help='재분류 주기 (기본: 7)')
    parser.add_argument('--oa-threshold',       type=float, default=0.1,
                        help='Overlap Area 임계값 (기본: 0.1)')
    parser.add_argument('--cohens-d-threshold', type=float, default=0.8,
                        help="Cohen's d 임계값 (기본: 0.8)")
    args = parser.parse_args()

    run(
        lookback_days=args.lookback_days,
        refresh_days=args.refresh_days,
        oa_threshold=args.oa_threshold,
        cohens_d_threshold=args.cohens_d_threshold,
    )
