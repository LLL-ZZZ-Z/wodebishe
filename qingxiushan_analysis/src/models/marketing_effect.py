# -*- coding: utf-8 -*-
"""
内容营销效果分析模型
功能：内容类型分类 + 互动指标对比 + 相关性分析
"""
import pandas as pd
import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr

# ===================== 1. 系统路径配置 =====================
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# ===================== 2. 加载数据 =====================
print("=== 1. 加载数据 ===")
data_path = os.path.join(ROOT_DIR, "data/processed/text_preprocess_df.csv")
if not os.path.exists(data_path):
    print(f"❌ 数据文件不存在：{data_path}")
    sys.exit(1)

df = pd.read_csv(data_path, encoding="utf-8")
# 补充互动指标（无则生成模拟数据）
if "like_num" not in df.columns:
    print("⚠️  无真实互动数据，生成模拟数据")
    import numpy as np
    np.random.seed(42)
    df["like_num"] = np.random.randint(0, 100, len(df))
    df["comment_num"] = np.random.randint(0, 50, len(df))
    df["collect_num"] = np.random.randint(0, 30, len(df))

# 标准化互动指标
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
df["like_num_norm"] = scaler.fit_transform(df[["like_num"]])
df["comment_num_norm"] = scaler.fit_transform(df[["comment_num"]])
df["collect_num_norm"] = scaler.fit_transform(df[["collect_num"]])

# ===================== 3. 内容类型分类 =====================
print("\n=== 2. 内容类型自动分类 ===")
def classify_content_type(text):
    """
    根据关键词分类营销内容：
    - 景观类：樱花、风景、景点、花园
    - 活动类：活动、演出、节日、优惠
    - 攻略类：攻略、路线、游玩、打卡
    - 其他类：剩余内容
    """
    if pd.isna(text):
        return "其他类"
    text = str(text).lower()
    if any(word in text for word in ["樱花", "风景", "景点", "花园", "植物", "自然"]):
        return "景观类"
    elif any(word in text for word in ["活动", "演出", "节日", "优惠", "折扣", "免费"]):
        return "活动类"
    elif any(word in text for word in ["攻略", "路线", "游玩", "打卡", "北门", "东门"]):
        return "攻略类"
    else:
        return "其他类"

df["content_type"] = df["content"].apply(classify_content_type)
content_count = df["content_type"].value_counts()
print("✅ 内容类型分布：")
for ctype, count in content_count.items():
    print(f"  {ctype}：{count}条（{(count/len(df))*100:.1f}%）")

# ===================== 4. 不同内容类型的互动效果对比 =====================
print("\n=== 3. 互动效果对比 ===")
# 按内容类型聚合互动指标
interaction_metrics = df.groupby("content_type")[
    ["like_num_norm", "comment_num_norm", "collect_num_norm"]
].mean()
print("📊 各内容类型标准化互动量（越高效果越好）：")
print(interaction_metrics.round(2))

# 找出最优内容类型
best_type = interaction_metrics["like_num_norm"].idxmax()
best_value = interaction_metrics["like_num_norm"].max()
print(f"🏆 最优营销内容类型：{best_type}（点赞量：{best_value:.2f}）")

# ===================== 5. 相关性分析（时间特征 vs 互动量） =====================
print("\n=== 4. 时间特征与互动量相关性 ===")
# 补充时间特征（无则填充）
if "year" not in df.columns:
    df["year"] = 2025
if "month" not in df.columns:
    df["month"] = 3
if "is_holiday" not in df.columns:
    df["is_holiday"] = 0

# 计算皮尔逊相关系数（修复核心：处理常量警告+变量名错误）
time_cols = ["year", "month", "is_holiday"]
corr_results = {}
for col in time_cols:
    # 检查列是否为常量（避免警告）
    if df[col].nunique() == 1:
        print(f"  {col}：列值为常量，无法计算相关性")
        corr_results[col] = (0.0, 1.0)  # 赋值默认值
        continue
    # 计算相关系数
    corr, p_value = pearsonr(df[col], df["like_num_norm"])
    corr_results[col] = (corr, p_value)
    # 修复：p → p_value（核心错误）
    print(f"  {col} vs 点赞量：相关系数={corr:.2f}，P值={p_value:.4f}（{'显著' if p_value<0.05 else '不显著'}）")

# ===================== 6. 可视化 =====================
print("\n=== 5. 生成可视化图表 ===")
plt.rcParams["font.family"] = ["SimHei", "Microsoft YaHei"]
plt.rcParams["axes.unicode_minus"] = False

fig_dir = os.path.join(ROOT_DIR, "figures")
os.makedirs(fig_dir, exist_ok=True)

# 绘制双图（互动对比+相关性热图）
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# 子图1：内容类型互动对比
interaction_metrics.plot(
    kind="bar",
    ax=ax1,
    colormap="Set2",
    width=0.8
)
ax1.set_title("不同内容类型的互动效果对比", fontsize=14, fontweight="bold")
ax1.set_xlabel("内容类型", fontsize=12)
ax1.set_ylabel("标准化互动量", fontsize=12)
ax1.tick_params(axis="x", rotation=45)
ax1.legend(title="互动指标", loc="upper right")

# 子图2：时间特征相关性热图
corr_df = df[["year", "month", "is_holiday", "like_num_norm"]].corr()
sns.heatmap(
    corr_df,
    ax=ax2,
    annot=True,
    cmap="RdBu",
    fmt=".2f",
    vmin=-1,
    vmax=1
)
ax2.set_title("时间特征与点赞量相关性", fontsize=14, fontweight="bold")

# 保存图表
plt.tight_layout()
fig_path = os.path.join(fig_dir, "marketing_effect_analysis.png")
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
print(f"✅ 营销效果图表已保存：{fig_path}")

# ===================== 7. 营销结论 =====================
print("\n=== 6. 营销效果总结 ===")
print(f"🎯 最优内容类型：{best_type}（建议重点投放）")
holiday_corr = corr_results["is_holiday"][0]
if holiday_corr > 0.3:
    print(f"🎈 节假日投放建议：节假日互动量显著更高（相关系数={holiday_corr:.2f}），建议节假日加大投放")
else:
    print(f"🎈 节假日投放建议：节假日互动量无显著优势（相关系数={holiday_corr:.2f}），可常规投放")

print("\n🎉 内容营销效果分析模型运行完成！")
print(f"📈 图表保存路径：{fig_path}")