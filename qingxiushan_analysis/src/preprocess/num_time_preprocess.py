# src/preprocess/num_time_preprocess.py
# 数值/时间特征预处理：适配携程2019-2026年数据 + 全年份节假日
import re
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from src.utils.common_utils import logger
from config.preprocess_config import SCORE_COL, TIME_COL, INTERACT_COLS, SCALE_COLS, TIME_FEATURES

# ===================== 2019-2026年核心节假日配置（覆盖分析时间范围） =====================
HOLIDAYS = {
    2019: [
        "2019-01-01", "2019-02-04", "2019-02-05", "2019-02-06", "2019-02-07", "2019-02-08", "2019-02-09", "2019-02-10",
        "2019-04-05", "2019-05-01", "2019-05-02", "2019-05-03", "2019-06-07", "2019-09-13", "2019-09-14", "2019-09-15",
        "2019-10-01", "2019-10-02", "2019-10-03", "2019-10-04", "2019-10-05", "2019-10-06", "2019-10-07"
    ],
    2020: [
        "2020-01-01", "2020-01-24", "2020-01-25", "2020-01-26", "2020-01-27", "2020-01-28", "2020-01-29", "2020-01-30",
        "2020-04-04", "2020-05-01", "2020-05-02", "2020-05-03", "2020-05-04", "2020-05-05", "2020-06-25", "2020-06-26",
        "2020-06-27", "2020-10-01", "2020-10-02", "2020-10-03", "2020-10-04", "2020-10-05", "2020-10-06", "2020-10-07"
    ],
    2021: [
        "2021-01-01", "2021-02-11", "2021-02-12", "2021-02-13", "2021-02-14", "2021-02-15", "2021-02-16", "2021-02-17",
        "2021-04-03", "2021-04-04", "2021-04-05", "2021-05-01", "2021-05-02", "2021-05-03", "2021-06-14", "2021-09-19",
        "2021-09-20", "2021-09-21", "2021-10-01", "2021-10-02", "2021-10-03", "2021-10-04", "2021-10-05", "2021-10-06",
        "2021-10-07"
    ],
    2022: [
        "2022-01-01", "2022-01-31", "2022-02-01", "2022-02-02", "2022-02-03", "2022-02-04", "2022-02-05", "2022-02-06",
        "2022-04-03", "2022-04-04", "2022-04-05", "2022-05-01", "2022-05-02", "2022-05-03", "2022-05-04", "2022-05-05",
        "2022-06-03", "2022-09-10", "2022-09-11", "2022-09-12", "2022-10-01", "2022-10-02", "2022-10-03", "2022-10-04",
        "2022-10-05", "2022-10-06", "2022-10-07"
    ],
    2023: [
        "2023-01-01", "2023-01-22", "2023-01-23", "2023-01-24", "2023-01-25", "2023-01-26", "2023-01-27", "2023-01-28",
        "2023-04-05", "2023-05-01", "2023-05-02", "2023-05-03", "2023-05-04", "2023-05-05", "2023-06-22", "2023-06-23",
        "2023-06-24", "2023-09-29", "2023-09-30", "2023-10-01", "2023-10-02", "2023-10-03", "2023-10-04", "2023-10-05",
        "2023-10-06", "2023-10-07"
    ],
    2024: [
        "2024-01-01", "2024-02-10", "2024-02-11", "2024-02-12", "2024-02-13", "2024-02-14", "2024-02-15", "2024-02-16",
        "2024-04-04", "2024-05-01", "2024-05-02", "2024-05-03", "2024-05-04", "2024-05-05", "2024-06-10", "2024-09-15",
        "2024-09-16", "2024-09-17", "2024-10-01", "2024-10-02", "2024-10-03", "2024-10-04", "2024-10-05", "2024-10-06",
        "2024-10-07"
    ],
    2025: [
        "2025-01-01", "2025-01-29", "2025-01-30", "2025-01-31", "2025-02-01", "2025-02-02", "2025-02-03", "2025-02-04",
        "2025-04-05", "2025-05-01", "2025-05-02", "2025-05-03", "2025-05-04", "2025-05-05", "2025-06-01", "2025-09-07",
        "2025-09-08", "2025-09-09", "2025-10-01", "2025-10-02", "2025-10-03", "2025-10-04", "2025-10-05", "2025-10-06",
        "2025-10-07"
    ],
    2026: [
        "2026-01-01", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20", "2026-02-21", "2026-02-22", "2026-02-23",
        "2026-04-05", "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05", "2026-06-20", "2026-09-26",
        "2026-09-27", "2026-09-28", "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04", "2026-10-05", "2026-10-06",
        "2026-10-07"
    ]
}

# 合并所有年份节假日为列表
ALL_HOLIDAYS = [date for year in HOLIDAYS.values() for date in year]


def clean_numeric_data(df):
    """
    第一步：清洗数值数据（处理空值、异常值）
    :param df: 文本预处理后的DataFrame
    :return: 清洗后的数值数据
    """
    logger.info("===== 开始数值数据清洗 =====")
    # 1. 处理空值（用0填充互动数据，用均值填充评分）
    for col in INTERACT_COLS:
        df[col] = df[col].fillna(0).astype(int)  # 互动数：空值→0
    score_mean = df[SCORE_COL].fillna(0).mean()  # 评分均值
    df[SCORE_COL] = df[SCORE_COL].fillna(score_mean).round(1)  # 评分：空值→均值

    # 2. 处理异常值（评分>5或<0的修正为均值）
    df.loc[df[SCORE_COL] > 5, SCORE_COL] = score_mean
    df.loc[df[SCORE_COL] < 0, SCORE_COL] = score_mean

    # 3. 处理互动数据异常值（超过99分位数的修正为99分位数）
    for col in INTERACT_COLS:
        q99 = df[col].quantile(0.99)
        df.loc[df[col] > q99, col] = q99

    logger.info("数值数据清洗完成（空值/异常值处理）")
    return df


def normalize_numeric_data(df, scale_cols=SCALE_COLS):
    """
    第二步：数值数据标准化（Z-score标准化，均值0，方差1）
    :param df: 清洗后的DataFrame
    :param scale_cols: 需要标准化的列（配置文件定义）
    :return: 原数据+标准化列
    """
    logger.info(f"开始数值标准化，待标准化列：{scale_cols}")
    scaler = StandardScaler()
    # 标准化
    df[scale_cols] = scaler.fit_transform(df[scale_cols])
    # 重命名标准化列（加_norm后缀，区分原始列）
    for col in scale_cols:
        df.rename(columns={col: f"{col}_norm"}, inplace=True)
    logger.info("数值标准化完成")
    return df


def extract_time_features(df, time_col=TIME_COL):
    """
    第三步：提取时间特征（适配携程时间格式 + 过滤2019-2026年数据）
    :param df: 数值清洗后的DataFrame
    :param time_col: 时间列名（默认pub_time）
    :return: 原数据+时间特征列
    """
    logger.info("===== 开始时间特征提取 =====")

    # 核心：清洗携程时间字符串，提取YYYY-MM-DD
    def clean_ctrip_time(time_str):
        if pd.isna(time_str):
            return None
        time_str = str(time_str).strip()
        # 正则匹配 20xx-xx-xx 格式的日期（忽略后缀）
        match = re.search(r'(\d{4}-\d{2}-\d{2})', time_str)
        return match.group(1) if match else None

    # 应用时间清洗
    df[time_col] = df[time_col].apply(clean_ctrip_time)

    # 转换为datetime格式（只保留日期）
    df[time_col] = pd.to_datetime(df[time_col], format="%Y-%m-%d", errors="coerce")

    # 过滤1：无有效时间的行
    df = df[df[time_col].notna()].reset_index(drop=True)
    logger.info(f"过滤无效时间后剩余数据：{len(df)}行")

    # 过滤2：仅保留2019-2026年的数据（核心需求）
    df = df[
        (df[time_col].dt.year >= 2019) &
        (df[time_col].dt.year <= 2026)
        ].reset_index(drop=True)
    logger.info(f"过滤2019-2026年数据后剩余：{len(df)}行")

    # 提取基础时间特征
    df["year"] = df[time_col].dt.year  # 年
    df["month"] = df[time_col].dt.month  # 月
    df["day"] = df[time_col].dt.day  # 日
    df["weekday"] = df[time_col].dt.weekday  # 星期几（0=周一，6=周日）

    # 提取衍生时间特征
    df["is_weekend"] = df["weekday"].apply(lambda x: 1 if x >= 5 else 0)  # 是否周末（1=是，0=否）
    # 是否节假日（适配2019-2026全年份）
    df["date_str"] = df[time_col].dt.strftime("%Y-%m-%d")
    df["is_holiday"] = df["date_str"].apply(lambda x: 1 if x in ALL_HOLIDAYS else 0)

    # 保留配置文件指定的时间特征，删除临时列
    time_features_keep = [f for f in TIME_FEATURES if f in df.columns]
    df = df.drop(columns=["date_str", "weekday"], errors="ignore")

    logger.info(f"时间特征提取完成，提取特征：{time_features_keep}")
    return df


def process_num_time_features(df):
    """
    整合数值+时间特征预处理流程
    :param df: 文本预处理后的DataFrame
    :return: 数值/时间预处理完成的DataFrame
    """
    # 1. 数值清洗
    df = clean_numeric_data(df)
    # 2. 数值标准化
    df = normalize_numeric_data(df)
    # 3. 时间特征提取
    df = extract_time_features(df)
    logger.info("===== 数值/时间特征预处理完成 =====")
    return df


# 测试函数（单独运行该文件时验证）
if __name__ == "__main__":
    from src.utils.db_utils import read_ugc_data
    from src.preprocess.text_preprocess import extract_text_features

    # 1. 读取UGC数据+文本预处理
    ugc_df = read_ugc_data()
    text_df = extract_text_features(ugc_df)
    # 2. 数值/时间预处理
    num_time_df = process_num_time_features(text_df)
    # 验证结果
    logger.info("数值/时间预处理结果示例：")
    print("标准化数值列示例：")
    print(num_time_df[["score_norm", "like_num_norm", "comment_num_norm"]].head(2))
    print("\n时间特征列示例：")
    print(num_time_df[["year", "month", "is_weekend", "is_holiday"]].head(2))
    # 打印年份分布，验证2019-2026过滤效果
    print("\n2019-2026年数据年份分布：")
    print(num_time_df["year"].value_counts().sort_index())