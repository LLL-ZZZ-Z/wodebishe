# test_text_preprocess.py：测试文本预处理功能
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.db_utils import read_ugc_data
from src.preprocess.text_preprocess import extract_text_features

if __name__ == "__main__":
    # 1. 从MySQL读取UGC数据
    ugc_df = read_ugc_data()
    print(f"原始UGC数据量：{len(ugc_df)}行")

    # 2. 执行文本预处理
    processed_df = extract_text_features(ugc_df)

    # 3. 验证结果
    print("\n===== 文本预处理结果验证 =====")
    print(f"预处理后数据量：{len(processed_df)}行")
    print("\n原始文本示例：")
    print(processed_df["content"].iloc[0])
    print("\n清洗后文本示例：")
    print(processed_df["clean_text"].iloc[0])
    print("\n分词结果示例：")
    print(processed_df["cut_words"].iloc[0])
    print("\nTF-IDF特征前5列示例：")
    tfidf_cols = [col for col in processed_df.columns if col.startswith("tfidf_")][:5]
    print(processed_df[tfidf_cols].head(2))