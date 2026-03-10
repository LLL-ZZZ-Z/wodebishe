# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
青秀山数据纯清洗脚本 - 无数据库操作
适配实际文件路径，仅输出清洗后的CSV
"""
import pandas as pd
import numpy as np
import re
import os

# ===================== 核心配置（你的实际文件路径）=====================
RAW_FILE_PATHS = {
    'ctrip_all': r"D:\python\毕设\qxs_data_crawl\crawl_scripts\data\comments\qingxiushan_8810_全部评论_按时间排序.csv",
    'ctrip_bad': r"D:\python\毕设\qxs_data_crawl\crawl_scripts\data\comments\qingxiushan_8810_差评.csv",
    'weather': r"D:\python\毕设\qxs_data_crawl\crawl_scripts\data\comments\nanning_weather_2019_2026.csv",
    'wechat': r"D:\python\毕设\qxs_data_crawl\crawl_scripts\data\comments\wechat_青秀山_710篇_error_20260309_175657.xlsx",
    'xhs': r"D:\python\毕设\qxs_data_crawl\crawl_scripts\data\comments\小红书_青秀山笔记_200条_20260226174021.xlsx"
}
# 清洗结果输出目录（自动创建）
CLEAN_OUTPUT_DIR = r"D:\python\毕设\qxs_data_crawl\crawl_scripts\data\cleaned_data"

# ===================== 通用清洗工具函数 =====================
def create_output_dir():
    """自动创建清洗结果目录，避免路径不存在"""
    if not os.path.exists(CLEAN_OUTPUT_DIR):
        os.makedirs(CLEAN_OUTPUT_DIR)
    print(f"清洗结果将保存至：{CLEAN_OUTPUT_DIR}")
    return CLEAN_OUTPUT_DIR

def clean_text(text):
    """清洗文本：去除空格、换行、制表符、特殊符号，保留中文/数字/常用标点"""
    if pd.isna(text):
        return ""
    text = str(text).strip().replace('\n', '').replace('\r', '').replace('\t', '')
    # 保留中文、数字、字母和常用中文标点
    text = re.sub(r'[^\u4e00-\u9fa50-9a-zA-Z，。！？：；""''()（）、·]', '', text)
    return text

def drop_duplicate(df, key_cols):
    """按核心字段去重，打印去重日志"""
    before_count = len(df)
    df = df.drop_duplicates(subset=key_cols, keep='first')
    after_count = len(df)
    drop_count = before_count - after_count
    if drop_count > 0:
        print(f"→ 去重完成：删除{drop_count}条重复数据，剩余{after_count}条")
    else:
        print(f"→ 无重复数据，当前共{after_count}条")
    return df

# ===================== 各数据源专项清洗 =====================
def clean_ctrip_comments():
    """清洗携程评论（合并全部评论+差评，避免重复）"""
    print("\n【1/4 开始清洗携程评论数据】")
    # 读取全部评论
    try:
        df_all = pd.read_csv(RAW_FILE_PATHS['ctrip_all'], encoding='utf-8-sig')
        print(f"携程全部评论原始数据：{len(df_all)}条")
    except FileNotFoundError:
        print(f"错误：未找到携程全部评论文件 → {RAW_FILE_PATHS['ctrip_all']}")
        return None
    # 读取差评数据并合并
    try:
        df_bad = pd.read_csv(RAW_FILE_PATHS['ctrip_bad'], encoding='utf-8-sig')
        print(f"携程差评原始数据：{len(df_bad)}条")
        df = pd.concat([df_all, df_bad], ignore_index=True)
        print(f"合并后总数据：{len(df)}条")
    except FileNotFoundError:
        print(f"提示：未找到携程差评文件，仅处理全部评论")
        df = df_all
    # 核心清洗步骤
    df = df[df['评论文本'].apply(lambda x: len(str(x).strip()) > 0)]  # 过滤空评论
    df['评论文本'] = df['评论文本'].apply(clean_text)  # 清洗评论文本
    # 修复评分异常值（0分替换为列均值，保留1位小数）
    score_cols = ['景色评分', '趣味评分', '性价比评分']
    for col in score_cols:
        if col in df.columns:
            mean_score = df[df[col] > 0][col].mean()
            df[col] = df[col].replace(0, round(mean_score, 1))
    # 统一格式
    df['发布时间'] = df['发布时间'].apply(lambda x: x if re.match(r'\d{4}-\d{2}', str(x)) else '未知')
    df['IP属地'] = df['IP属地'].replace('未知', '未公开')
    # 去重：用户ID+评论文本+发布时间（唯一标识一条评论）
    df = drop_duplicate(df, ['用户ID', '评论文本', '发布时间'])
    # 保存清洗结果
    output_path = os.path.join(create_output_dir(), "cleaned_ctrip_comments.csv")
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"携程评论清洗完成 → 保存至：{output_path}")
    return df

def clean_weather_data():
    """清洗南宁天气数据"""
    print("\n【2/4 开始清洗天气数据】")
    try:
        df = pd.read_csv(RAW_FILE_PATHS['weather'], encoding='utf-8-sig')
        print(f"天气数据原始数据：{len(df)}条")
    except FileNotFoundError:
        print(f"错误：未找到天气文件 → {RAW_FILE_PATHS['weather']}")
        return None
    # 核心清洗步骤
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')  # 转换日期格式
    df = df.dropna(subset=['日期'])  # 过滤异常日期
    df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')  # 统一为YYYY-MM-DD
    # 提取温度数字，补全空值
    temp_cols = ['最高温(°C)', '最低温(°C)']
    for col in temp_cols:
        df[col] = df[col].apply(lambda x: re.findall(r'\d+', str(x))[0] if re.findall(r'\d+', str(x)) else np.nan)
        df[col] = df[col].fillna(method='ffill').astype(int)
    # 修复AQI空值，统一为数字
    df['空气质量指数(AQI)'] = df['空气质量指数(AQI)'].apply(lambda x: x if str(x).isdigit() else 0)
    df['空气质量指数(AQI)'] = df['空气质量指数(AQI)'].astype(int)
    # 清洗风力风向
    df['风力风向'] = df['风力风向'].apply(clean_text)
    # 去重：按日期去重（同一天仅保留1条）
    df = drop_duplicate(df, ['日期'])
    # 保存清洗结果
    output_path = os.path.join(create_output_dir(), "cleaned_weather.csv")
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"天气数据清洗完成 → 保存至：{output_path}")
    return df

def clean_wechat_articles():
    """清洗微信公众号文章数据"""
    print("\n【3/4 开始清洗微信文章数据】")
    try:
        df = pd.read_excel(RAW_FILE_PATHS['wechat'], engine='openpyxl')
        print(f"微信文章原始数据：{len(df)}条")
    except FileNotFoundError:
        print(f"错误：未找到微信文件 → {RAW_FILE_PATHS['wechat']}")
        return None
    # 字段重命名（英文→中文，适配后续入库）
    rename_map = {'title':'文章标题', 'link':'文章链接', 'account':'公众号名',
                  'pub_time':'发布时间', 'abstract':'文章摘要', 'keyword':'关键词',
                  'page':'爬取页码', 'crawl_time':'采集时间'}
    df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
    # 核心清洗步骤
    df = df.dropna(subset=['文章标题', '文章链接'])  # 过滤核心空值
    # 清洗所有文本字段
    for col in ['文章标题', '公众号名', '文章摘要']:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
    df['发布时间'] = df['发布时间'].apply(lambda x: clean_text(x) if pd.notna(x) else '未知')
    # 去重：按文章链接去重（同一链接仅保留1条）
    df = drop_duplicate(df, ['文章链接'])
    # 保存清洗结果
    output_path = os.path.join(create_output_dir(), "cleaned_wechat_articles.csv")
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"微信文章清洗完成 → 保存至：{output_path}")
    return df

def clean_xhs_notes():
    """清洗小红书笔记数据"""
    print("\n【4/4 开始清洗小红书笔记数据】")
    try:
        df = pd.read_excel(RAW_FILE_PATHS['xhs'], engine='openpyxl')
        print(f"小红书笔记原始数据：{len(df)}条")
    except FileNotFoundError:
        print(f"错误：未找到小红书文件 → {RAW_FILE_PATHS['xhs']}")
        return None
    # 核心清洗步骤
    df = df.dropna(subset=['笔记标题', '笔记链接'])  # 过滤核心空值
    # 清洗文本字段
    for col in ['博主名', '笔记标题', '笔记内容']:
        df[col] = df[col].apply(clean_text)
    # 提取互动数数字，统一为int
    interact_cols = ['点赞数', '收藏数', '评论数']
    for col in interact_cols:
        df[col] = df[col].apply(lambda x: re.findall(r'\d+', str(x))[0] if re.findall(r'\d+', str(x)) else 0)
        df[col] = df[col].astype(int)
    # 去重：按笔记链接去重（同一笔记仅保留1条）
    df = drop_duplicate(df, ['笔记链接'])
    # 保存清洗结果
    output_path = os.path.join(create_output_dir(), "cleaned_xhs_notes.csv")
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"小红书笔记清洗完成 → 保存至：{output_path}")
    return df

# ===================== 执行清洗主程序 =====================
if __name__ == "__main__":
    print("="*60)
    print("          青秀山数据纯清洗程序 - 无数据库操作")
    print("="*60)
    # 依次执行所有数据源清洗
    clean_ctrip_comments()
    clean_weather_data()
    clean_wechat_articles()
    clean_xhs_notes()
    print("\n" + "="*60)
    print("✅ 所有数据清洗完成！清洗结果已保存至指定目录")
    print("="*60)