# qxs_data_crawl/data_loader.py
#读取已清洗 / 入库的数据）
import pandas as pd
import pymysql
import os

# --------------------------
# 配置：替换成你自己的路径/数据库信息
# --------------------------
# 1. 本地清洗后CSV文件路径（你clean.py输出的文件路径）
CSV_PATHS = {
    "ctrip": "./crawl_scripts/data/cleaned_data/cleaned_ctrip_comments.csv",  # 携程清洗后数据
    "xhs": "./crawl_scripts/data/cleaned_data/cleaned_xhs_comments.csv",  # 小红书清洗后数据
    "weather": "./crawl_scripts/data/cleaned_data/cleaned_weather.csv"  # 天气清洗后数据
}

# 2. 数据库配置（你mysql.py里的配置）
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "你的数据库密码",
    "database": "qxs_db",  # 你创建的数据库名
    "charset": "utf8mb4"
}


# --------------------------
# 功能1：读取本地清洗后的CSV（优先用这个，更简单）
# --------------------------
def load_from_csv(data_type="ctrip"):
    """
    读取你已清洗好的CSV数据
    :param data_type: 数据类型（ctrip/xhs/weather）
    :return: DataFrame（结构化数据）
    """
    try:
        file_path = CSV_PATHS[data_type]
        if not os.path.exists(file_path):
            print(f"❌ 找不到文件：{file_path}，请检查路径是否正确")
            return pd.DataFrame()

        df = pd.read_csv(file_path, encoding="utf-8")
        print(f"✅ 成功读取{data_type}数据，共{len(df)}条记录")
        return df
    except Exception as e:
        print(f"❌ 读取{data_type}CSV失败：{e}")
        return pd.DataFrame()


# --------------------------
# 功能2：读取MySQL数据库（如果你已入库，可选）
# --------------------------
def load_from_mysql(sql):
    """
    读取你已入库的数据
    :param sql: 查询SQL
    :return: DataFrame
    """
    try:
        conn = pymysql.connect(**DB_CONFIG)
        df = pd.read_sql(sql, conn)
        conn.close()
        print(f"✅ 成功读取数据库数据，共{len(df)}条记录")
        return df
    except Exception as e:
        print(f"❌ 读取数据库失败：{e}")
        return pd.DataFrame()


# --------------------------
# 测试：验证数据读取是否成功
# --------------------------
if __name__ == "__main__":
    # 测试读取携程清洗后数据（先跑这个，确认能读到数据）
    ctrip_df = load_from_csv("ctrip")
    if not ctrip_df.empty:
        print("📌 携程数据前5行：")
        print(ctrip_df.head())

#测试读取天气数据
    weather_df = load_from_csv("weather")
    if not weather_df.empty:
        print("📌 天气数据前5行：")
        print(weather_df.head())