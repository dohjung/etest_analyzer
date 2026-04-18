"""
Sample Data Generator for Bin Similarity Monitor
=================================================
실제 DB 없이 테스트할 수 있는 샘플 데이터 생성.

실행 방법:
    python generate_sample_data.py
    python generate_sample_data.py --date 2025-04-17 --lots 50

생성 파일:
    ./data/sample/fail_data.csv   - Fail bin 프로파일 구축용
    ./data/sample/pass_data.csv   - Pass unit 분석용 (드리프트 시나리오 포함)
"""

import os
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ============================================================
# 설정
# ============================================================

SAMPLE_DIR  = './data/sample'
RANDOM_SEED = 42

# 테스트 파라미터 (반도체 테스트 항목)
TEST_FEATURES = ['VREAD', 'IWRITE', 'TPROG', 'VERASE', 'ICELL', 'VTH', 'IBIAS', 'TREAD']

# Fail bin 정의: {hard_bin: {feature: (mean, std)}}
FAIL_BIN_PARAMS = {
    10: {
        'VREAD':  (0.8,  0.05), 'IWRITE': (2.5, 0.20), 'TPROG':  (15.0, 1.00),
        'VERASE': (1.2,  0.10), 'ICELL':  (1.0, 0.10), 'VTH':    (0.5,  0.05),
        'IBIAS':  (0.3,  0.03), 'TREAD':  (8.0, 0.50),
    },
    20: {
        'VREAD':  (0.5,  0.04), 'IWRITE': (1.8, 0.15), 'TPROG':  (10.0, 0.80),
        'VERASE': (1.0,  0.08), 'ICELL':  (1.2, 0.12), 'VTH':    (0.4,  0.04),
        'IBIAS':  (0.8,  0.08), 'TREAD':  (6.0, 0.40),
    },
    30: {
        'VREAD':  (1.0,  0.06), 'IWRITE': (2.0, 0.18), 'TPROG':  (12.0, 0.90),
        'VERASE': (1.5,  0.12), 'ICELL':  (1.8, 0.15), 'VTH':    (0.3,  0.03),
        'IBIAS':  (0.4,  0.04), 'TREAD':  (9.0, 0.60),
    },
}

# Pass unit 정규 파라미터
PASS_NORMAL_PARAMS = {
    'VREAD':  (1.2, 0.05), 'IWRITE': (1.5, 0.10), 'TPROG':  (8.0, 0.50),
    'VERASE': (0.9, 0.07), 'ICELL':  (0.7, 0.06), 'VTH':    (0.7, 0.04),
    'IBIAS':  (0.2, 0.02), 'TREAD':  (4.0, 0.30),
}

# Fail bin 프로파일용: bin당 die 수
N_FAIL_DIES_PER_BIN = 60

# Pass unit: 드리프트 시나리오
# 전체 lot 중 마지막 DRIFT_START_RATIO 비율부터 점진적으로 bin 10 방향 drift
DRIFT_START_RATIO = 0.70   # 전체 lot의 70% 이후부터 drift 시작
DRIFT_MAX_DIE_FRAC = 0.40  # drift lot 중 최대 40% die에 영향
DRIFT_MAX_STRENGTH = 0.70  # 최대 drift 강도 (정상 <-> fail 평균 보간)


# ============================================================
# 데이터 생성 함수
# ============================================================

def generate_fail_data(seed: int = RANDOM_SEED) -> pd.DataFrame:
    """Fail bin 프로파일 구축용 데이터 생성 (long format)"""
    rng = np.random.default_rng(seed)
    rows = []
    base_date = datetime(2025, 1, 1)

    for bin_id, feat_params in FAIL_BIN_PARAMS.items():
        lot_id = f'LOT_FAIL_{bin_id:02d}'
        for die_idx in range(N_FAIL_DIES_PER_BIN):
            die_id = f'DIE_F{bin_id:02d}_{die_idx:04d}'
            start_time = base_date + timedelta(
                days=die_idx // 20, hours=die_idx % 24
            )
            for feat, (mean, std) in feat_params.items():
                rows.append({
                    'lot_id':      lot_id,
                    'die_id':      die_id,
                    'start_time':  start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'test_txt':    feat,
                    'pin':         0,
                    'test_result': float(rng.normal(mean, std)),
                    'hard_bin':    bin_id,
                    'soft_bin':    bin_id,  # pass가 아닌 bin
                })

    return pd.DataFrame(rows)


def generate_pass_data(target_date: str,
                       n_lots: int,
                       n_dies_per_lot: int,
                       seed: int = RANDOM_SEED) -> pd.DataFrame:
    """
    Pass unit 분석용 데이터 생성 (long format).

    구조:
        - 처음 DRIFT_START_RATIO 비율 lot: 정상 파라미터
        - 이후 lot: 점진적으로 bin 10 방향으로 drift
    """
    rng = np.random.default_rng(seed + 1)
    rows = []
    drift_bin_params = FAIL_BIN_PARAMS[10]
    base_time = datetime.strptime(target_date, '%Y-%m-%d')
    drift_start_idx = int(n_lots * DRIFT_START_RATIO)

    for lot_idx in range(n_lots):
        lot_id = f'LOT_{lot_idx:04d}'
        lot_time = base_time + timedelta(minutes=lot_idx * 20)

        # drift 강도: drift_start 이후 점진적으로 증가
        if lot_idx >= drift_start_idx:
            progress = (lot_idx - drift_start_idx) / max(1, n_lots - drift_start_idx - 1)
            die_drift_frac = DRIFT_MAX_DIE_FRAC * progress
            drift_strength = DRIFT_MAX_STRENGTH * progress
        else:
            die_drift_frac = 0.0
            drift_strength = 0.0

        for die_idx in range(n_dies_per_lot):
            die_id = f'DIE_{lot_idx:04d}_{die_idx:04d}'
            start_time = lot_time + timedelta(seconds=die_idx * 10)
            apply_drift = (die_drift_frac > 0) and (rng.random() < die_drift_frac)

            for feat in TEST_FEATURES:
                normal_mean, normal_std = PASS_NORMAL_PARAMS[feat]
                fail_mean, _           = drift_bin_params[feat]

                if apply_drift:
                    mean = normal_mean + drift_strength * (fail_mean - normal_mean)
                else:
                    mean = normal_mean

                rows.append({
                    'lot_id':      lot_id,
                    'die_id':      die_id,
                    'start_time':  start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'test_txt':    feat,
                    'pin':         0,
                    'test_result': float(rng.normal(mean, normal_std)),
                    'soft_bin':    1,
                    'hard_bin':    1,
                })

    return pd.DataFrame(rows)


# ============================================================
# 메인
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='샘플 데이터 생성기')
    parser.add_argument('--date',          type=str, default='2025-04-17',
                        help='Pass unit 분석 기준 날짜 (YYYY-MM-DD)')
    parser.add_argument('--lots',          type=int, default=50,
                        help='생성할 pass unit lot 수 (기본: 50)')
    parser.add_argument('--dies-per-lot',  type=int, default=200,
                        help='lot당 die 수 (기본: 200)')
    args = parser.parse_args()

    os.makedirs(SAMPLE_DIR, exist_ok=True)
    drift_start = int(args.lots * DRIFT_START_RATIO)
    print(f"샘플 데이터 생성 시작 (날짜: {args.date})")
    print(f"  Lot 구성: 총 {args.lots}개 (정상: 0~{drift_start-1}, "
          f"드리프트: {drift_start}~{args.lots-1})")

    # Fail data
    print("  [1/2] Fail bin 데이터 생성 중...")
    df_fail = generate_fail_data()
    fail_path = os.path.join(SAMPLE_DIR, 'fail_data.csv')
    df_fail.to_csv(fail_path, index=False)
    print(f"        저장: {fail_path}  "
          f"({len(df_fail):,} rows / {df_fail['die_id'].nunique()} dies / "
          f"{df_fail['hard_bin'].nunique()} bins)")

    # Pass data
    print("  [2/2] Pass unit 데이터 생성 중...")
    df_pass = generate_pass_data(
        target_date=args.date,
        n_lots=args.lots,
        n_dies_per_lot=args.dies_per_lot,
    )
    pass_path = os.path.join(SAMPLE_DIR, 'pass_data.csv')
    df_pass.to_csv(pass_path, index=False)
    print(f"        저장: {pass_path}  "
          f"({len(df_pass):,} rows / {df_pass['die_id'].nunique()} dies / "
          f"{df_pass['lot_id'].nunique()} lots)")

    print("샘플 데이터 생성 완료")
    print(f"\n다음 명령으로 분석을 실행하세요:")
    print(f"  python bin_similarity_monitor_sample.py --date {args.date}")


if __name__ == '__main__':
    main()
