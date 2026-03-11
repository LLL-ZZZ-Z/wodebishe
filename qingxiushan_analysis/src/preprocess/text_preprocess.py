# src/preprocess/text_preprocess.py
# 文本数据预处理：清洗→分词→去停用词→TF-IDF特征提取
import re
import jieba
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from src.utils.common_utils import logger, load_stopwords
from config.preprocess_config import STOPWORDS_PATH, TFIDF_VOCAB_SIZE, TEXT_COL

# 提前加载停用词库（避免重复加载）
stopwords = load_stopwords(STOPWORDS_PATH)


def clean_text(text):
    """
    第一步：清洗文本（去特殊符号/空格/换行，保留中文）
    :param text: 原始评论文本
    :return: 清洗后的纯中文文本
    """
    # 处理空值
    if pd.isna(text):
        return ""
    # 转为字符串+去首尾空格
    text = str(text).strip()
    # 1. 去除特殊符号、数字、字母（只保留中文）
    text = re.sub(r"[^\u4e00-\u9fa5]", " ", text)
    # 2. 去除多余空格（多个空格→单个空格）
    text = re.sub(r"\s+", " ", text).strip()
    # 3. 去除过短文本（无意义）
    if len(text) < 2:
        return ""
    return text


def jieba_cut_text(text):
    """
    第二步：结巴分词+去停用词
    :param text: 清洗后的文本
    :return: 分词后的词语列表
    """
    if not text:  # 空文本直接返回空
        return []
    # 1. 结巴分词（精确模式）
    words = jieba.lcut(text, cut_all=False)
    # 2. 去停用词+去单字（单字无特征意义）
    words_filtered = [word for word in words if word not in stopwords and len(word) > 1]
    return words_filtered


def extract_text_features(df, text_col=TEXT_COL):
    """
    第三步：文本特征提取（TF-IDF）
    :param df: 原始UGC数据DataFrame
    :param text_col: 文本列名（默认content）
    :return: 原数据+清洗分词列+TF-IDF特征DataFrame
    """
    logger.info("===== 开始文本预处理 =====")

    # 1. 文本清洗
    df["clean_text"] = df[text_col].apply(clean_text)
    logger.info(f"文本清洗完成，有效文本数：{len(df[df['clean_text'] != ''])}")

    # 2. 结巴分词
    df["cut_words"] = df["clean_text"].apply(jieba_cut_text)
    # 过滤空分词结果
    df = df[df["cut_words"].apply(len) > 0].reset_index(drop=True)
    logger.info(f"分词完成，过滤空文本后剩余数据：{len(df)}行")

    # 3. 转换为空格分隔的字符串（适配TF-IDF输入格式）
    df["corpus"] = df["cut_words"].apply(lambda x: " ".join(x))

    # 4. TF-IDF特征提取（核心文本特征）
    tfidf = TfidfVectorizer(
        max_features=TFIDF_VOCAB_SIZE,  # 特征维度（配置文件定义）
        ngram_range=(1, 2),  # 提取单字+双字特征（更丰富）
        lowercase=False  # 中文无需小写
    )
    tfidf_matrix = tfidf.fit_transform(df["corpus"])
    # 转换为DataFrame（特征列名：tfidf_0, tfidf_1...）
    tfidf_df = pd.DataFrame(
        tfidf_matrix.toarray(),
        columns=[f"tfidf_{i}" for i in range(tfidf_matrix.shape[1])]
    )
    logger.info(f"TF-IDF特征提取完成，特征维度：{tfidf_matrix.shape[1]}")

    # 5. 合并原数据+TF-IDF特征
    result_df = pd.concat([df, tfidf_df], axis=1)
    logger.info("===== 文本预处理完成 =====")

    return result_df


# 测试函数（单独运行该文件时验证）
if __name__ == "__main__":
    # 导入MySQL读取函数
    from src.utils.db_utils import read_ugc_data

    # 读取UGC数据
    ugc_df = read_ugc_data()
    # 执行文本预处理
    text_processed_df = extract_text_features(ugc_df)
    # 打印结果示例
    logger.info("文本预处理结果示例：")
    print("核心列预览：")
    print(text_processed_df[["content", "clean_text", "cut_words", "tfidf_0"]].head(2))