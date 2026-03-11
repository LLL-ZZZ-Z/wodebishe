# -*- coding: utf-8 -*-
"""
游客兴趣分析模型（完整版）
功能：LDA主题建模 + K-means聚类 + 中文可视化
"""
import pandas as pd
import jieba
import re
import os
import sys
from gensim import corpora, models
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler

# ===================== 1. 系统路径配置（核心修复） =====================
# 获取项目根目录
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 添加项目根目录到系统路径（解决模块导入问题）
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# ===================== 2. 数据加载与预处理 =====================
try:
    # 导入自定义工具函数
    from src.utils.db_utils import read_ugc_data
    from src.preprocess.text_preprocess import extract_text_features

    # 读取原始UGC数据
    print("=== 1. 加载原始数据 ===")
    ugc_df = read_ugc_data()

    # 执行文本预处理（分词/去停用词等）
    print("=== 2. 执行文本预处理 ===")
    df = extract_text_features(ugc_df)

    # 保存预处理后的数据（供其他模型复用）
    save_path = os.path.join(ROOT_DIR, "data/processed/text_preprocess_df.csv")
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False, encoding="utf-8")
    print(f"✅ 预处理数据已保存：{save_path}")

except ImportError as e:
    print(f"⚠️  模块导入失败：{e}")
    print("⚠️  请确认src/utils/db_utils.py和src/preprocess/text_preprocess.py文件存在")
    sys.exit(1)

# ===================== 3. 文本深度预处理（LDA专用） =====================
# 加载停用词（无文件则使用默认停用词）
stopwords_path = os.path.join(ROOT_DIR, "stopwords.txt")
if os.path.exists(stopwords_path):
    stopwords = [line.strip() for line in open(stopwords_path, "r", encoding="utf-8").readlines()]
else:
    # 默认停用词表（适配中文文本分析）
    stopwords = [
        "的", "了", "是", "我", "你", "他", "她", "它", "在", "和", "有", "就", "都", "而", "及",
        "与", "也", "还", "不", "没", "很", "非常", "比较", "一个", "这个", "那个", "什么",
        "怎么", "哪里", "这里", "那里", "然后", "所以", "因为", "但是", "如果", "就是", "只是"
    ]


# 定义文本清洗函数
def clean_text(text):
    """
    文本清洗：保留中文 + 分词 + 去停用词
    """
    if pd.isna(text):
        return []
    # 仅保留中文字符
    text = re.sub(r"[^\u4e00-\u9fa5]", "", str(text))
    # 结巴分词
    words = jieba.lcut(text)
    # 去停用词 + 过滤单字
    words = [w for w in words if w not in stopwords and len(w) > 1]
    return words


# 执行文本清洗
df["clean_words"] = df["content"].apply(clean_text)
# 过滤空文本
df = df[df["clean_words"].apply(len) > 0]
print(f"✅ 有效文本数据量：{len(df)}条")

# 补充时间特征（防止字段缺失）
if "year" not in df.columns:
    df["year"] = 2025
if "month" not in df.columns:
    df["month"] = 3
if "is_weekend" not in df.columns:
    df["is_weekend"] = 0
if "is_holiday" not in df.columns:
    df["is_holiday"] = 0

# ===================== 4. LDA主题建模（提取游客关注主题） =====================
print("\n=== 3. 开始LDA主题建模 ===")
# 构建词典和语料库
dictionary = corpora.Dictionary(df["clean_words"])
corpus = [dictionary.doc2bow(words) for words in df["clean_words"]]

# 训练LDA模型（5个主题，可根据需求调整）
lda_model = models.LdaModel(
    corpus=corpus,
    id2word=dictionary,
    num_topics=5,  # 主题数量
    random_state=42,  # 随机种子（保证结果可复现）
    iterations=100,  # 迭代次数
    passes=10,  # 遍历语料库次数
    alpha="auto",  # 自动调整alpha参数
    eta="auto"  # 自动调整eta参数
)

# 输出主题关键词（可视化前先看结果）
print("\n=== LDA主题建模结果（TOP10关键词）===")
topic_keywords = []
for idx, topic in lda_model.print_topics(num_words=10):
    print(f"主题{idx + 1}: {topic}")
    topic_keywords.append(topic)

# 为每条评论分配最相关的主题
df["topic_id"] = [max(lda_model.get_document_topics(bow), key=lambda x: x[1])[0] for bow in corpus]

# ===================== 5. K-means聚类（划分兴趣群体） =====================
print("\n=== 4. 开始K-means聚类 ===")
# 特征融合：TF-IDF文本特征 + 时间特征
# 5.1 TF-IDF文本特征（500维）
tfidf = TfidfVectorizer(max_features=500)
text_features = tfidf.fit_transform([" ".join(words) for words in df["clean_words"]]).toarray()

# 5.2 时间特征（标准化）
time_cols = ["year", "month", "is_weekend", "is_holiday"]
time_features = df[time_cols].values
time_features = StandardScaler().fit_transform(time_features)

# 5.3 特征合并
all_features = pd.concat([
    pd.DataFrame(text_features),
    pd.DataFrame(time_features)
], axis=1)

# 5.4 训练K-means模型（5个聚类）
kmeans = KMeans(
    n_clusters=5,
    random_state=42,
    n_init=10  # 多次初始化取最优结果
)
df["cluster_id"] = kmeans.fit_predict(all_features)

# ===================== 6. 可视化（中文标签+优化版） =====================
print("\n=== 5. 生成可视化图表 ===")
# 6.1 全局可视化配置（中文+美观）
plt.rcParams["font.family"] = ["SimHei", "Microsoft YaHei"]  # 中文显示
plt.rcParams["axes.unicode_minus"] = False  # 负号显示
plt.rcParams["figure.figsize"] = (14, 8)  # 图表大小
plt.rcParams["font.size"] = 11  # 基础字体大小

# 6.2 定义中文标签（核心优化）
# 群体标签（根据聚类结果定制）
cluster_cn_labels = [
    "综合体验型",
    "文化地标型",
    "休闲打卡型",
    "纯体验型",
    "极致体验型"
]
# 主题标签（根据LDA结果定制）
topic_cn_labels = [
    "整体体验评价",
    "游玩路线+拍照",
    "季节性景观",
    "自然景观+植物",
    "景区地标+城市名片"
]

# 6.3 构建交叉表（群体×主题）
cluster_topic_crosstab = pd.crosstab(df["cluster_id"], df["topic_id"])
# 转换为百分比（每行占比）
cluster_topic_pct = cluster_topic_crosstab.div(cluster_topic_crosstab.sum(axis=1), axis=0) * 100
# 替换标签为中文
cluster_topic_pct.index = cluster_cn_labels
cluster_topic_pct.columns = topic_cn_labels

# 6.4 绘制热图
fig, ax = plt.subplots(figsize=(14, 8))
sns.heatmap(
    cluster_topic_pct,
    ax=ax,
    annot=True,  # 显示数值
    fmt=".1f",  # 保留1位小数
    cmap="Reds",  # 红色系（越红越关注）
    linewidths=0.5,  # 网格线宽度
    cbar_kws={
        "label": "关注占比（%）",  # 色条标签
        "shrink": 0.8  # 色条缩放
    }
)

# 6.5 图表美化
ax.set_title(
    "青秀山游客兴趣群体-关注主题分布热图",
    fontsize=16,
    fontweight="bold",
    pad=20
)
ax.set_xlabel("游客关注主题", fontsize=14, labelpad=10)
ax.set_ylabel("游客兴趣群体", fontsize=14, labelpad=10)
# 横轴标签旋转45度（避免重叠）
plt.xticks(rotation=45, ha="right")
# 调整布局（防止标签被截断）
plt.tight_layout()

# 6.6 保存图表
fig_dir = os.path.join(ROOT_DIR, "figures")
os.makedirs(fig_dir, exist_ok=True)
fig_path = os.path.join(fig_dir, "interest_cluster_topic_cn.png")
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
print(f"✅ 中文标签热图已保存：{fig_path}")

# ===================== 7. 游客画像总结（中文） =====================
print("\n=== 6. 游客兴趣群体画像总结 ===")
for i, cluster_name in enumerate(cluster_cn_labels):
    cluster_data = df[df["cluster_id"] == i]
    # 核心特征
    top_topic_id = cluster_data["topic_id"].value_counts().idxmax()
    top_topic_name = topic_cn_labels[top_topic_id]
    sample_count = len(cluster_data)
    sample_pct = (sample_count / len(df)) * 100
    # 输出画像
    print(f"\n【{cluster_name}】")
    print(f"- 占比：{sample_pct:.1f}%（{sample_count}人）")
    print(f"- 核心关注：{top_topic_name}")
    print(f"- 典型特征：{cluster_data['clean_words'].iloc[0][:5]}...")

# ===================== 8. 运行完成 =====================
print("\n🎉 游客兴趣分析模型（完整版）运行完成！")
print(f"📊 可视化图表：{fig_path}")
print(f"📝 预处理数据：{save_path}")