# config/preprocess_config.py
# 适配MySQL数据读取，删除CSV路径，新增核心特征列定义
# ===================== 核心特征列定义（适配MySQL读取的UGC数据） =====================
TEXT_COL = "content"       # 统一文本列名（携程comment_text/小红书note_content）
SCORE_COL = "score"        # 统一评分列（携程star_rating）
TIME_COL = "pub_time"      # 统一时间列（携程publish_time）
INTERACT_COLS = ["like_num", "comment_num", "collect_num"]  # 统一互动列

# ===================== 特征工程参数 =====================
TFIDF_VOCAB_SIZE = 2000  # 文本TF-IDF特征维度
TIME_FEATURES = ["year", "month", "is_holiday", "is_weekend"]  # 提取的时间特征
SCALE_COLS = INTERACT_COLS + [SCORE_COL]  # 需要标准化的数值列

# ===================== 输出路径 =====================
# 预处理后的特征数据保存为CSV（供后续建模使用）
PROCESSED_FEATURE_PATH = "D:/python/毕设/qingxiushan_analysis/data/processed/standard_feature_set.csv"
STOPWORDS_PATH = "D:/python/毕设/qingxiushan_analysis/stopwords/cn_stopwords.txt"  # 停用词路径