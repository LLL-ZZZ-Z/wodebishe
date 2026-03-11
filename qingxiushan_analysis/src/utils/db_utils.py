# src/utils/db_utils.py
import pymysql
import pandas as pd
from pymysql.err import OperationalError
from src.utils.common_utils import logger

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123mysql',
    'db': 'qingxiushan',
    'charset': 'utf8mb4'
}

def get_mysql_conn():
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        logger.info("✅ 成功连接MySQL数据库(qingxiushan)")
        return conn
    except OperationalError as e:
        logger.error(f"❌ MySQL连接失败！原因：{e}")
        logger.error("请检查：1.MySQL服务是否启动 2.用户名/密码是否正确")
        raise e
    except Exception as e:
        logger.error(f"❌ 数据库连接异常：{e}")
        raise e

def read_mysql_table(table_name, sql=None):
    if sql is None:
        sql = f"SELECT * FROM {table_name};"
    try:
        conn = get_mysql_conn()
        df = pd.read_sql(sql, conn)
        conn.close()
        logger.info(f"✅ 成功读取表{table_name}，共{len(df)}行数据")
        return df
    except Exception as e:
        logger.error(f"❌ 读取表{table_name}失败：{e}")
        raise e

# 【核心修改】新增公众号数据读取，合并携程+小红书+公众号三源数据
def read_ugc_data():
    """
    读取核心UGC数据（携程评论+小红书笔记+微信公众号文章）
    统一列名/格式，生成多源融合的预处理数据集
    """
    # 1. 携程评论数据（原逻辑，微调列名）
    ctrip_df = read_mysql_table("ctrip_comments")[
        ["id", "comment_text", "publish_time", "star_rating", "useful_count", "collect_cnt"]
    ]
    ctrip_df.rename(
        columns={
            "comment_text": "content",
            "publish_time": "pub_time",
            "star_rating": "score",
            "useful_count": "like_num",
            "collect_cnt": "collect_num"
        },
        inplace=True
    )
    ctrip_df["comment_num"] = 0  # 携程无评论数，补0
    ctrip_df["source"] = "ctrip"  # 标记：携程
    ctrip_df["content_type"] = "comment"  # 内容类型：用户评论

    # 2. 小红书笔记数据（原逻辑）
    xhs_df = read_mysql_table("xhs_notes")[
        ["id", "note_content", "pub_time", "like_count", "collect_count", "comment_count"]
    ]
    xhs_df.rename(
        columns={
            "note_content": "content",
            "like_count": "like_num",
            "collect_count": "collect_num",
            "comment_count": "comment_num"
        },
        inplace=True
    )
    xhs_df["score"] = 0  # 小红书无评分，补0
    xhs_df["source"] = "xhs"  # 标记：小红书
    xhs_df["content_type"] = "note"  # 内容类型：种草笔记

    # 【新增】3. 微信公众号文章数据（适配你的700条公众号数据）
    wechat_df = read_mysql_table("wechat_articles")[
        ["id", "article_content", "pub_time", "read_count", "like_count", "comment_count"]
    ]
    wechat_df.rename(
        columns={
            "article_content": "content",
            "pub_time": "pub_time",
            "like_count": "like_num",
            "comment_count": "comment_num",
            "read_count": "read_num"  # 公众号新增：阅读量（核心营销指标）
        },
        inplace=True
    )
    wechat_df["score"] = 0  # 公众号无评分，补0
    wechat_df["collect_num"] = 0  # 公众号无收藏数，补0
    wechat_df["source"] = "wechat"  # 标记：公众号
    wechat_df["content_type"] = "article"  # 内容类型：官方文章

    # 4. 三源数据合并（统一结构，过滤空内容）
    ugc_df = pd.concat([ctrip_df, xhs_df, wechat_df], ignore_index=True)
    ugc_df = ugc_df[ugc_df["content"].notna() & (ugc_df["content"] != "")].reset_index(drop=True)
    # 补充通用空列，避免后续建模报错
    for col in ["read_num"]:
        if col not in ugc_df.columns:
            ugc_df[col] = 0

    logger.info(f"✅ 三源UGC数据合并完成（携程+小红书+公众号），共{len(ugc_df)}行")
    logger.info(f"📊 各平台数据量：{ugc_df['source'].value_counts().to_dict()}")
    return ugc_df

# 原有天气数据读取（保留，新增客流关联分析用）
def read_weather_data():
    return read_mysql_table("nanning_weather")

# 【新增】读取景区基础客流数据（若有，无则注释，后续可补）
def read_flow_data():
    """读取青秀山景区客流数据（对接景区票务/监控数据）"""
    try:
        flow_df = read_mysql_table("scenic_flow")
        logger.info("✅ 成功读取景区客流数据")
        return flow_df
    except Exception as e:
        logger.warning(f"⚠️  未找到客流数据表，后续客流关联分析将跳过：{e}")
        return None