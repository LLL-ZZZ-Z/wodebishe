# test_num_time_preprocess.py：测试数值/时间特征预处理
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.db_utils import read_ugc_data
from src.preprocess.text_preprocess import extract_text_features
from src.preprocess.num_time_preprocess import process_num_time_features

if __name__ == "__main__":
    # 1. 读取UGC数据+文本预处理
    ugc_df = read_ugc_data()
    text_processed_df = extract_text_features(ugc_df)
    print(f"文本预处理后数据量：{len(text_processed_df)}行")

    # 2. 数值/时间特征预处理
    num_time_processed_df = process_num_time_features(text_processed_df)

    # 3. 验证结果
    print("\n===== 数值/时间预处理结果验证 =====")
    print(f"最终数据量：{len(num_time_processed_df)}行")

    # 验证数值标准化
    print("\n标准化数值特征（均值≈0，方差≈1）：")
    norm_cols = [col for col in num_time_processed_df.columns if col.endswith("_norm")]
    for col in norm_cols:
        mean = num_time_processed_df[col].mean().round(2)
        std = num_time_processed_df[col].std().round(2)
        print(f"{col} - 均值：{mean}，方差：{std}")

    # 验证时间特征
    print("\n时间特征示例：")
    time_cols = ["year", "month", "is_weekend", "is_holiday"]
    print(num_time_processed_df[time_cols].head(5))

# 假设text_df是文本预处理后的DataFrame（包含content/clean_words/year/month等字段）
# 若没有text_df，先重新执行文本预处理逻辑：
from src.utils.db_utils import read_ugc_data
from src.preprocess.text_preprocess import extract_text_features

ugc_df = read_ugc_data()
text_df = extract_text_features(ugc_df)  # 重新生成文本预处理数据

# 2. 保存到指定路径
save_path = "data/processed/text_preprocess_df.csv"
# 确保目录存在
import os
os.makedirs(os.path.dirname(save_path), exist_ok=True)
# 保存文件
text_df.to_csv(save_path, index=False, encoding="utf-8")
print(f"✅ 文本预处理结果已保存：{save_path}")