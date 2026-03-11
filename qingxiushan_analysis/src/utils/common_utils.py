# 通用工具类：文件读取/保存、日志打印、停用词加载
import pandas as pd
import logging
import os

# 配置日志（运行时能看到进度，方便排错）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("utils")

def read_csv_file(file_path):
    """读取CSV数据文件（核心工具）"""
    if not os.path.exists(file_path):
        logger.error(f"❌ 找不到文件：{file_path}")
        raise FileNotFoundError(f"文件不存在：{file_path}")
    # 读取数据，指定编码避免中文乱码
    df = pd.read_csv(file_path, encoding="utf-8-sig")
    logger.info(f"✅ 成功读取数据：{file_path}，共{len(df)}行")
    return df

def save_csv_file(df, file_path):
    """保存CSV文件（自动创建文件夹）"""
    # 自动创建输出目录（不用手动建）
    dir_path = os.path.dirname(file_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        logger.info(f"📁 自动创建目录：{dir_path}")
    # 保存数据
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    logger.info(f"✅ 成功保存数据：{file_path}，共{len(df)}行")

def load_stopwords(stopwords_path):
    """加载中文停用词库（文本预处理用）"""
    if not os.path.exists(stopwords_path):
        logger.error(f"❌ 找不到停用词文件：{stopwords_path}")
        raise FileNotFoundError(f"停用词文件不存在：{stopwords_path}")
    # 读取停用词
    with open(stopwords_path, "r", encoding="utf-8") as f:
        stopwords = [line.strip() for line in f if line.strip()]
    # 新增旅游领域停用词（根据青秀山场景补充）
    travel_stopwords = ["青秀山", "景区", "南宁", "打卡", "游玩", "去", "来"]
    stopwords.extend(travel_stopwords)
    stopwords = set(stopwords)  # 去重
    logger.info(f"✅ 成功加载停用词：共{len(stopwords)}个")
    return stopwords