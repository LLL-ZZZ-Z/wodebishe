# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
青秀山数据纯MySQL入库脚本 - 修复NaN空值问题
读取清洗后的CSV，自动建表+批量入库，密码：123mysql
"""
import pandas as pd
import pymysql
from pymysql.err import OperationalError
import os
import numpy as np

# ===================== 核心配置 =====================
# 1. 清洗后CSV文件的路径（与清洗脚本输出目录一致）
CLEAN_CSV_PATH = r"D:\python\毕设\qxs_data_crawl\crawl_scripts\data\cleaned_data"
# 2. MySQL配置（密码固定为123mysql）
MYSQL_CONFIG = {
    'host': 'localhost',  # 本地数据库填localhost，远程填IP
    'user': 'root',  # 你的MySQL用户名（默认root，需确认）
    'password': '123mysql',  # 固定密码，无需修改
    'db': 'qingxiushan'  # 数据库名（自动创建）
}
# 3. 清洗后CSV文件映射
CLEAN_FILES = {
    'ctrip': os.path.join(CLEAN_CSV_PATH, "cleaned_ctrip_comments.csv"),
    'weather': os.path.join(CLEAN_CSV_PATH, "cleaned_weather.csv"),
    'wechat': os.path.join(CLEAN_CSV_PATH, "cleaned_wechat_articles.csv"),
    'xhs': os.path.join(CLEAN_CSV_PATH, "cleaned_xhs_notes.csv")
}


# ===================== MySQL核心操作 =====================
def init_mysql_tables():
    """自动创建/重建青秀山数据库+4张表，字符集utf8mb4（支持中文/表情）"""
    create_table_sql = """
    -- 创建数据库（不存在则创建）
    CREATE DATABASE IF NOT EXISTS qingxiushan 
    DEFAULT CHARACTER SET utf8mb4 
    DEFAULT COLLATE utf8mb4_unicode_ci;

    USE qingxiushan;

    -- 1. 携程评论表
    DROP TABLE IF EXISTS ctrip_comments;
    CREATE TABLE ctrip_comments (
        id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
        user_id VARCHAR(50) COMMENT '用户ID',
        comment_text TEXT COMMENT '评论文本',
        publish_time VARCHAR(20) COMMENT '发布时间(YYYY-MM)',
        ip_location VARCHAR(50) COMMENT 'IP属地',
        scenery_score FLOAT(2,1) COMMENT '景色评分',
        fun_score FLOAT(2,1) COMMENT '趣味评分',
        cost_score FLOAT(2,1) COMMENT '性价比评分',
        useful_count INT DEFAULT 0 COMMENT '赞同数',
        collect_cnt INT DEFAULT 0 COMMENT '收藏数',
        tourist_type VARCHAR(30) COMMENT '出行类型',
        star_rating INT COMMENT '评论星级',
        comment_type VARCHAR(10) COMMENT '评论类型(好评/中评/差评)',
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='青秀山携程评论表';

    -- 2. 天气数据表
    DROP TABLE IF EXISTS nanning_weather;
    CREATE TABLE nanning_weather (
        id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
        weather_date VARCHAR(20) UNIQUE COMMENT '日期(YYYY-MM-DD)',
        max_temp INT COMMENT '最高温(°C)',
        min_temp INT COMMENT '最低温(°C)',
        weather_status VARCHAR(50) COMMENT '天气状况',
        wind VARCHAR(50) COMMENT '风力风向',
        aqi INT DEFAULT 0 COMMENT '空气质量指数(AQI)',
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='南宁天气数据表（青秀山）';

    -- 3. 微信文章表
    DROP TABLE IF EXISTS wechat_articles;
    CREATE TABLE wechat_articles (
        id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
        article_title VARCHAR(255) COMMENT '文章标题',
        article_link VARCHAR(512) UNIQUE COMMENT '文章链接',
        account_name VARCHAR(100) COMMENT '公众号名',
        publish_time VARCHAR(50) COMMENT '发布时间',
        article_abstract TEXT COMMENT '文章摘要',
        keyword VARCHAR(50) COMMENT '爬取关键词',
        crawl_page INT DEFAULT 1 COMMENT '爬取页码',
        crawl_time VARCHAR(50) COMMENT '采集时间',
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='青秀山相关微信文章表';

    -- 4. 小红书笔记表
    DROP TABLE IF EXISTS xhs_notes;
    CREATE TABLE xhs_notes (
        id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
        author_name VARCHAR(100) COMMENT '博主名',
        note_title VARCHAR(255) COMMENT '笔记标题',
        note_content TEXT COMMENT '笔记内容',
        like_count INT DEFAULT 0 COMMENT '点赞数',
        collect_count INT DEFAULT 0 COMMENT '收藏数',
        comment_count INT DEFAULT 0 COMMENT '评论数',
        note_link VARCHAR(512) UNIQUE COMMENT '笔记链接',
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='青秀山相关小红书笔记表';
    """
    try:
        # 连接MySQL（先不指定数据库，支持创建新库）
        conn = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            charset='utf8mb4',
            connect_timeout=10
        )
        cursor = conn.cursor()
        # 执行建表SQL（按分号分割，跳过空行）
        for sql in create_table_sql.split(';'):
            sql = sql.strip()
            if sql:
                cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ MySQL初始化完成：已创建qingxiushan数据库+4张表")
    except OperationalError as e:
        print(f"❌ MySQL连接失败！原因：{e}")
        print("请检查：1.MySQL服务是否启动 2.用户名/密码是否正确 3.本地能否访问MySQL")
        exit(1)
    except Exception as e:
        print(f"❌ 建表失败！原因：{e}")
        exit(1)


def clean_nan_values(df, col_mapping):
    """
    清理DataFrame中的NaN/空值，适配MySQL插入
    :param df: 读取后的CSV数据
    :param col_mapping: 列名映射
    :return: 清理后的DataFrame
    """
    # 只处理需要入库的列
    valid_cols = [k for k in col_mapping.keys() if k in df.columns]
    df = df[valid_cols].copy()

    # 遍历每一列，根据类型替换NaN
    for col in df.columns:
        dtype = df[col].dtype
        # 字符串类型：NaN替换为空字符串
        if dtype == 'object':
            df[col] = df[col].fillna('').astype(str)
            # 去除字符串中的nan文本（避免"nan"字符串入库）
            df[col] = df[col].replace('nan', '')
        # 数值类型（int/float）：NaN替换为0
        elif dtype in [np.int64, np.float64]:
            df[col] = df[col].fillna(0)
        # 布尔类型：NaN替换为False
        elif dtype == 'bool':
            df[col] = df[col].fillna(False)

    return df


def batch_insert_mysql(csv_file, table_name, col_mapping):
    """
    批量插入CSV数据到MySQL（修复NaN空值问题）
    :param csv_file: 清洗后的CSV文件路径
    :param table_name: MySQL目标表名
    :param col_mapping: CSV列名 → MySQL字段名 映射
    """
    # 检查CSV文件是否存在
    if not os.path.exists(csv_file):
        print(f"❌ 未找到清洗后的文件 → {csv_file}，跳过该表入库")
        return
    # 读取CSV文件
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    if len(df) == 0:
        print(f"❌ {csv_file} 无数据，跳过该表入库")
        return

    # 核心修复：清理所有NaN/空值
    df_clean = clean_nan_values(df, col_mapping)
    if len(df_clean) == 0:
        print(f"❌ {table_name} 清理后无有效数据，跳过入库")
        return

    # 映射到MySQL字段名
    valid_mysql_cols = [col_mapping[k] for k in df_clean.columns]
    # 转换数据为元组列表（确保无NaN）
    data = []
    for row in df_clean.values:
        # 逐个值检查，确保无NaN
        clean_row = []
        for val in row:
            if pd.isna(val) or val == 'nan':
                clean_row.append('' if isinstance(val, str) else 0)
            else:
                clean_row.append(val)
        data.append(tuple(clean_row))

    # 批量插入数据库
    try:
        conn = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            db=MYSQL_CONFIG['db'],
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        # 构造插入SQL（存在则跳过，避免重复）
        sql = f"INSERT INTO {table_name} ({','.join(valid_mysql_cols)}) VALUES ({','.join(['%s'] * len(valid_mysql_cols))}) ON DUPLICATE KEY UPDATE id=id"
        # 分批次插入（每次100条，避免数据量过大）
        batch_size = 100
        success_count = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(sql, batch)
            success_count += len(batch)
        conn.commit()
        print(f"✅ 表{table_name}：成功插入{success_count}条数据")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ 表{table_name}入库失败！原因：{str(e)[:200]}")  # 只显示前200字符，避免过长


# ===================== 执行入库主程序 =====================
if __name__ == "__main__":
    print("=" * 60)
    print("        青秀山数据纯MySQL入库程序 - 密码：123mysql")
    print("        （已修复NaN空值入库问题）")
    print("=" * 60)
    # 第一步：初始化MySQL库和表
    print("\n【第一步】初始化MySQL数据库和表")
    init_mysql_tables()
    # 第二步：批量插入各表数据
    print("\n【第二步】开始批量入库数据")
    # 2.1 携程评论入库
    batch_insert_mysql(
        CLEAN_FILES['ctrip'],
        'ctrip_comments',
        {
            '用户ID': 'user_id', '评论文本': 'comment_text', '发布时间': 'publish_time',
            'IP属地': 'ip_location', '景色评分': 'scenery_score', '趣味评分': 'fun_score',
            '性价比评分': 'cost_score', '赞同数': 'useful_count', '收藏数': 'collect_cnt',
            '出行类型': 'tourist_type', '评论星级': 'star_rating', '评论类型': 'comment_type'
        }
    )
    # 2.2 天气数据入库
    batch_insert_mysql(
        CLEAN_FILES['weather'],
        'nanning_weather',
        {
            '日期': 'weather_date', '最高温(°C)': 'max_temp', '最低温(°C)': 'min_temp',
            '天气': 'weather_status', '风力风向': 'wind', '空气质量指数(AQI)': 'aqi'
        }
    )
    # 2.3 微信文章入库
    batch_insert_mysql(
        CLEAN_FILES['wechat'],
        'wechat_articles',
        {
            '文章标题': 'article_title', '文章链接': 'article_link', '公众号名': 'account_name',
            '发布时间': 'publish_time', '文章摘要': 'article_abstract', '关键词': 'keyword',
            '爬取页码': 'crawl_page', '采集时间': 'crawl_time'
        }
    )
    # 2.4 小红书笔记入库
    batch_insert_mysql(
        CLEAN_FILES['xhs'],
        'xhs_notes',
        {
            '博主名': 'author_name', '笔记标题': 'note_title', '笔记内容': 'note_content',
            '点赞数': 'like_count', '收藏数': 'collect_count', '评论数': 'comment_count',
            '笔记链接': 'note_link'
        }
    )
    # 完成提示
    print("\n" + "=" * 60)
    print("✅ 所有数据入库操作执行完成！")
    print(f"可在MySQL的【{MYSQL_CONFIG['db']}】数据库中查看数据")
    print("=" * 60)