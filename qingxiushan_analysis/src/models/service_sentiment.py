# -*- coding: utf-8 -*-
"""
服务质量情感评估模型
功能：情感得分计算 + 负面关键词提取 + 服务短板定位
"""
import pandas as pd
import jieba
import re
import os
import sys
import matplotlib.pyplot as plt
from collections import Counter
import seaborn as sns

# ===================== 1. 系统路径配置 =====================
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# ===================== 2. 加载预处理数据 =====================
print("=== 1. 加载文本预处理数据 ===")
data_path = os.path.join(ROOT_DIR, "data/processed/text_preprocess_df.csv")
if not os.path.exists(data_path):
    print(f"❌ 数据文件不存在：{data_path}")
    print("❌ 请先运行tourist_interest.py生成数据")
    sys.exit(1)

df = pd.read_csv(data_path, encoding="utf-8")
print(f"✅ 加载数据量：{len(df)}条")

# ===================== 3. 加载情感词典（无文件则使用默认） =====================
# 定义默认情感词表（适配景区服务分析）
default_pos_words = [
    "好", "不错", "棒", "满意", "漂亮", "干净", "方便", "快捷", "贴心", "值得",
    "舒适", "优美", "开心", "愉快", "惊喜", "推荐", "赞", "优秀", "整洁", "完善"
]
default_neg_words = [
    "差", "不好", "糟糕", "失望", "脏", "乱", "差", "贵", "慢", "排队",
    "拥挤", "敷衍", "不专业", "不方便", "不值", "垃圾", "恶心", "破旧", "缺失"
]

# 加载自定义情感词典（优先）
pos_path = os.path.join(ROOT_DIR, "pos_words.txt")
neg_path = os.path.join(ROOT_DIR, "neg_words.txt")

if os.path.exists(pos_path):
    pos_words = [line.strip() for line in open(pos_path, "r", encoding="utf-8").readlines()]
else:
    pos_words = default_pos_words
    print(f"⚠️  未找到正面情感词典，使用默认词表：{pos_path}")

if os.path.exists(neg_path):
    neg_words = [line.strip() for line in open(neg_path, "r", encoding="utf-8").readlines()]
else:
    neg_words = default_neg_words
    print(f"⚠️  未找到负面情感词典，使用默认词表：{neg_path}")

# ===================== 4. 情感得分计算 =====================
print("\n=== 2. 计算情感得分 ===")


def calculate_sentiment(text):
    """
    计算情感得分：(正面词数 - 负面词数) / 总情感词数
    得分范围：[-1, 1]，正值=正面，负值=负面，0=中性
    """
    if pd.isna(text):
        return 0.0, 0, 0

    # 文本清洗
    text = re.sub(r"[^\u4e00-\u9fa5]", "", str(text))
    words = jieba.lcut(text)

    # 统计情感词
    pos_count = len([w for w in words if w in pos_words])
    neg_count = len([w for w in words if w in neg_words])

    # 避免除以0
    total = max(1, pos_count + neg_count)
    sentiment_score = (pos_count - neg_count) / total

    return sentiment_score, pos_count, neg_count


# 批量计算
df["sentiment_score"], df["pos_count"], df["neg_count"] = zip(*df["content"].apply(calculate_sentiment))


# 情感分类
def sentiment_label(score):
    if score > 0.2:
        return "正面"
    elif score < -0.2:
        return "负面"
    else:
        return "中性"


df["sentiment_label"] = df["sentiment_score"].apply(sentiment_label)

# ===================== 5. 负面评论关键词提取（定位服务短板） =====================
print("\n=== 3. 提取负面评论关键词 ===")
# 筛选负面评论
negative_df = df[df["sentiment_label"] == "负面"]
if len(negative_df) == 0:
    print("⚠️  未检测到负面评论")
    negative_keywords = []
else:
    # 提取负面关键词
    negative_text = negative_df["content"].tolist()
    negative_words_list = []

    for text in negative_text:
        text = re.sub(r"[^\u4e00-\u9fa5]", "", str(text))
        words = jieba.lcut(text)
        # 过滤停用词+保留负面词
        stopwords = ["的", "了", "是", "我", "你", "他", "在", "和", "有", "就", "都"]
        negative_words_list.extend([w for w in words if w in neg_words and w not in stopwords and len(w) > 1])

    # 统计TOP10负面关键词
    negative_keywords = Counter(negative_words_list).most_common(10)
    print("✅ 负面评论核心关键词（服务短板）：")
    for word, count in negative_keywords:
        print(f"  {word}: {count}次")

# ===================== 6. 可视化（中文+双图） =====================
print("\n=== 4. 生成可视化图表 ===")
plt.rcParams["font.family"] = ["SimHei", "Microsoft YaHei"]
plt.rcParams["axes.unicode_minus"] = False

# 创建图表目录
fig_dir = os.path.join(ROOT_DIR, "figures")
os.makedirs(fig_dir, exist_ok=True)

# 6.1 绘制双图（情感分布+负面关键词）
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# 子图1：情感分布饼图
sentiment_count = df["sentiment_label"].value_counts()
colors = ["#66b3ff", "#99ff99", "#ff9999"]  # 正面=蓝，中性=绿，负面=红
ax1.pie(
    sentiment_count.values,
    labels=sentiment_count.index,
    autopct="%1.1f%%",
    colors=colors,
    startangle=90,
    textprops={"fontsize": 11}
)
ax1.set_title("游客情感倾向分布", fontsize=14, fontweight="bold")

# 子图2：负面关键词条形图
if len(negative_keywords) > 0:
    words = [w[0] for w in negative_keywords]
    counts = [w[1] for w in negative_keywords]
    ax2.bar(words, counts, color="#ff6b6b")
    ax2.set_title("负面评论核心关键词（服务短板）", fontsize=14, fontweight="bold")
    ax2.set_xlabel("关键词", fontsize=12)
    ax2.set_ylabel("出现次数", fontsize=12)
    ax2.tick_params(axis="x", rotation=45)
else:
    ax2.text(0.5, 0.5, "无负面评论", ha="center", va="center", fontsize=14)
    ax2.set_title("负面评论核心关键词（服务短板）", fontsize=14, fontweight="bold")

# 调整布局+保存
plt.tight_layout()
fig_path = os.path.join(fig_dir, "service_sentiment_analysis.png")
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
print(f"✅ 情感分析图表已保存：{fig_path}")

# ===================== 7. 服务质量评估总结 =====================
print("\n=== 5. 服务质量评估总结 ===")
# 核心指标
total_count = len(df)
positive_count = len(df[df["sentiment_label"] == "正面"])
negative_count = len(df[df["sentiment_label"] == "负面"])
neutral_count = len(df[df["sentiment_label"] == "中性"])

positive_pct = (positive_count / total_count) * 100
negative_pct = (negative_count / total_count) * 100
avg_sentiment = df["sentiment_score"].mean()

print(f"📊 整体情感得分：{avg_sentiment:.2f}（正值=正面，负值=负面）")
print(f"👍 正面评论：{positive_count}条（{positive_pct:.1f}%）")
print(f"👎 负面评论：{negative_count}条（{negative_pct:.1f}%）")
print(f"😐 中性评论：{neutral_count}条（{(neutral_count / total_count) * 100:.1f}%）")

if len(negative_keywords) > 0:
    top3_problems = [w[0] for w in negative_keywords[:3]]
    print(f"🔍 核心服务短板：{top3_problems[0]}、{top3_problems[1]}、{top3_problems[2]}")
else:
    print("🔍 未发现明显服务短板")

print("\n🎉 服务质量情感评估模型运行完成！")
print(f"📈 图表保存路径：{fig_path}")