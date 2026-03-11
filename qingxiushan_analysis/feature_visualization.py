# feature_visualization.py
# 特征工程结果可视化（中文标题+字体适配）
import sys
import os
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import pandas as pd
import matplotlib.font_manager as fm

# ===================== 核心：全局中文字体配置（适配Windows） =====================
# 方案1：自动加载系统黑体（优先）
try:
    # 查找Windows系统黑体
    font_path = fm.findfont(fm.FontProperties(family='SimHei'))
    plt.rcParams['font.sans-serif'] = [font_path, 'SimHei', 'Microsoft YaHei']
except:
    # 方案2：手动指定字体路径（备用，需替换为你的字体路径）
    # 从Windows字体目录复制simhei.ttf到项目根目录
    font_path = os.path.join(os.path.dirname(__file__), 'simhei.ttf')
    if os.path.exists(font_path):
        font_prop = fm.FontProperties(fname=font_path)
        plt.rcParams['font.family'] = font_prop.get_name()
    else:
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']

plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
sns.set_style("whitegrid")

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.db_utils import read_ugc_data
from src.preprocess.text_preprocess import extract_text_features
from src.preprocess.num_time_preprocess import process_num_time_features

# 1. 读取并预处理数据
print("===== 加载特征工程数据 =====")
ugc_df = read_ugc_data()
text_df = extract_text_features(ugc_df)
final_df = process_num_time_features(text_df)


# ===================== 图表1：2019-2026年数据年份分布（核心） =====================
def plot_year_distribution(df):
    plt.figure(figsize=(12, 6))
    year_counts = df["year"].value_counts().sort_index()
    # 修复palette警告
    sns.barplot(x=year_counts.index.astype(str), y=year_counts.values, hue=year_counts.index.astype(str),
                palette="Blues_d", legend=False)

    # 添加数值标签
    for i, v in enumerate(year_counts.values):
        plt.text(i, v + 20, str(v), ha="center", fontsize=10, fontfamily='SimHei')

    plt.title("2019-2026年青秀山UGC数据年份分布", fontsize=14, fontweight="bold", fontfamily='SimHei')
    plt.xlabel("年份", fontsize=12, fontfamily='SimHei')
    plt.ylabel("数据量（条）", fontsize=12, fontfamily='SimHei')
    plt.xticks(fontsize=10, fontfamily='SimHei')
    plt.yticks(fontsize=10, fontfamily='SimHei')
    plt.tight_layout()
    plt.savefig("figures/year_distribution.png", dpi=300, bbox_inches="tight")
    print("✅ 年份分布图已保存：figures/year_distribution.png")


# ===================== 图表2：月度数据分布（节假日分析） =====================
def plot_month_distribution(df):
    plt.figure(figsize=(12, 6))
    month_counts = df["month"].value_counts().sort_index()
    sns.lineplot(x=month_counts.index.astype(str), y=month_counts.values, marker="o",
                 linewidth=2, color="#2E86AB")

    # 标记节假日月份
    holiday_months = [1, 2, 4, 5, 6, 9, 10]
    for month in holiday_months:
        if month in month_counts.index:
            plt.scatter(str(month), month_counts[month], color="red", s=80, zorder=5)

    plt.title("青秀山UGC数据月度分布（红色=节假日月份）", fontsize=14, fontweight="bold", fontfamily='SimHei')
    plt.xlabel("月份", fontsize=12, fontfamily='SimHei')
    plt.ylabel("数据量（条）", fontsize=12, fontfamily='SimHei')
    plt.xticks(range(1, 13), [str(i) for i in range(1, 13)], fontsize=10, fontfamily='SimHei')
    plt.yticks(fontsize=10, fontfamily='SimHei')
    plt.legend(["常规月份", "节假日月份"], loc="upper right", prop={'family': 'SimHei'})
    plt.tight_layout()
    plt.savefig("figures/month_distribution.png", dpi=300, bbox_inches="tight")
    print("✅ 月度分布图已保存：figures/month_distribution.png")


# ===================== 图表3：周末/工作日数据对比 =====================
def plot_weekend_vs_weekday(df):
    plt.figure(figsize=(8, 6))
    weekend_counts = df["is_weekend"].value_counts()
    labels = ["工作日", "周末"]
    sizes = [weekend_counts.get(0, 0), weekend_counts.get(1, 0)]
    colors = ["#A23B72", "#F18F01"]
    explode = (0.05, 0)  # 突出周末

    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct="%1.1f%%",
            shadow=True, startangle=90, textprops={"fontsize": 12, "fontfamily": 'SimHei'})
    plt.title("青秀山UGC数据周末/工作日分布", fontsize=14, fontweight="bold", fontfamily='SimHei')
    plt.axis("equal")  # 保证饼图为正圆形
    plt.tight_layout()
    plt.savefig("figures/weekend_weekday.png", dpi=300, bbox_inches="tight")
    print("✅ 周末/工作日对比图已保存：figures/weekend_weekday.png")


# ===================== 图表4：文本特征词云（核心语义） =====================
def plot_wordcloud(df):
    # 合并所有分词结果
    all_words = " ".join([" ".join(words) for words in df["cut_words"] if words])

    # 生成词云（指定中文字体）
    wc_kwargs = {
        "width": 1000,
        "height": 600,
        "background_color": "white",
        "max_words": 200,
        "colormap": "viridis",
        "random_state": 42
    }
    # 优先使用系统黑体
    try:
        wc_kwargs["font_path"] = fm.findfont(fm.FontProperties(family='SimHei'))
    except:
        # 备用字体路径
        if os.path.exists("simhei.ttf"):
            wc_kwargs["font_path"] = "simhei.ttf"

    wordcloud = WordCloud(**wc_kwargs).generate(all_words)

    plt.figure(figsize=(12, 8))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title("青秀山UGC文本核心特征词云", fontsize=14, fontweight="bold", pad=20, fontfamily='SimHei')
    plt.tight_layout()
    plt.savefig("figures/wordcloud.png", dpi=300, bbox_inches="tight")
    print("✅ 词云图已保存：figures/wordcloud.png")


# ===================== 主函数：生成所有图表 =====================
if __name__ == "__main__":
    # 创建图表保存目录
    os.makedirs("figures", exist_ok=True)

    # 生成所有可视化图表
    plot_year_distribution(final_df)
    plot_month_distribution(final_df)
    plot_weekend_vs_weekday(final_df)
    plot_wordcloud(text_df)  # 词云用文本预处理后的数据

    print("\n🎉 所有特征工程可视化图表已生成完成！")
    print("📁 图表保存路径：项目根目录/figures/")
    print("🔍 包含图表：年份分布、月度分布、周末/工作日对比、文本词云")