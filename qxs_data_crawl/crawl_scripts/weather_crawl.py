import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
from datetime import datetime
import urllib.parse

# --- 配置信息 ---
CITY_ID = "59431"
AREA_TYPE = "2"  # 根据您的截图，这个参数是2
# 正确的接口URL
AJAX_URL = "https://tianqi.2345.com/Pc/GetHistory"

START_YEAR = 2019
END_YEAR = datetime.now().year
END_MONTH = datetime.now().month

# 请求头必须完整，尤其是 Referer 和 X-Requested-With
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0',
    'Referer': f'https://tianqi.2345.com/wea_history/{CITY_ID}.htm',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6'
}


# 可选：从浏览器复制的Cookie，如果遇到访问限制可以加入
# COOKIES = "您的完整cookie字符串"

def fetch_month_data(year, month):
    """通过正确的AJAX接口获取单个月份数据"""
    # 构造参数，注意参数的key需要像截图那样进行URL编码
    # 但requests库会自动处理，我们直接使用字典即可
    params = {
        'areaInfo[areaId]': CITY_ID,
        'areaInfo[areaType]': AREA_TYPE,
        'date[year]': year,
        'date[month]': month
    }
    print(f"正在抓取: {year}年{month}月")

    try:
        # 如果需要使用cookie，可以加上 cookies=COOKIES
        response = requests.get(AJAX_URL, headers=HEADERS, params=params, timeout=15)

        if response.status_code != 200:
            print(f"接口访问失败，状态码: {response.status_code}")
            return None

        # 接口返回的是JSON，直接解析
        json_data = response.json()

        # 根据您最初提供的信息，code为1表示成功
        if json_data.get('code') != 1:
            print(f"接口返回错误: {json_data.get('msg')}")
            return None

        # 核心数据在 'data' 字段中，是一段HTML
        html_fragment = json_data['data']

        # 解析这段HTML
        soup = BeautifulSoup(html_fragment, 'html.parser')
        # 定位表格，根据您提供的HTML片段，class是 'history-table'
        table = soup.find('table', class_='history-table')

        if not table:
            print("在返回的HTML片段中未找到数据表格")
            # 可以打印片段来调试
            # print(html_fragment[:500])
            return None

        month_data = []
        rows = table.find_all('tr')[1:]  # 跳过表头行
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 6:
                continue

            # 提取并清洗数据
            date = cols[0].get_text(strip=True).split(' ')[0]  # 只取日期部分，去掉星期
            high = cols[1].get_text(strip=True).replace('°', '')
            low = cols[2].get_text(strip=True).replace('°', '')
            weather = cols[3].get_text(strip=True)
            wind = cols[4].get_text(strip=True)

            # 空气质量列可能包含数字和文字，例如 "40 优"，提取数字部分
            aqi_text = cols[5].get_text(strip=True)
            aqi = ''.join(filter(str.isdigit, aqi_text)) or ''

            month_data.append([date, high, low, weather, wind, aqi])

        print(f"成功获取 {len(month_data)} 条记录")
        return month_data

    except requests.exceptions.Timeout:
        print(f"请求超时: {year}年{month}月")
        return None
    except requests.exceptions.RequestException as e:
        print(f"网络请求错误: {e}")
        return None
    except Exception as e:
        print(f"解析 {year}年{month}月 数据时出错: {e}")
        return None


# --- 主程序 ---
def main():
    all_data = []
    total_months = 0
    success_months = 0

    for year in range(START_YEAR, END_YEAR + 1):
        # 确定每个年份需要抓取的月份范围
        start_month = 1
        end_month = 12
        if year == START_YEAR:
            start_month = 1  # 从1月开始
        if year == END_YEAR:
            end_month = END_MONTH  # 只到当前月

        for month in range(start_month, end_month + 1):
            total_months += 1
            data = fetch_month_data(year, month)

            if data:
                all_data.extend(data)
                success_months += 1

            # 礼貌性延时，避免请求过快导致IP被封
            time.sleep(1)  # 等待1秒

    # 保存为CSV文件
    if all_data:
        df = pd.DataFrame(all_data,
                          columns=['日期', '最高温(°C)', '最低温(°C)', '天气', '风力风向', '空气质量指数(AQI)'])
        # 按日期排序，确保数据是按时间顺序的
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期')
        df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')

        output_file = f'nanning_weather_{START_YEAR}_{END_YEAR}.csv'
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n抓取完成！")
        print(f"总月份数: {total_months}, 成功月份: {success_months}")
        print(f"总记录数: {len(df)} 条")
        print(f"数据已保存至: {output_file}")
    else:
        print("未抓取到任何数据，请检查网络或接口状态。")


if __name__ == "__main__":
    main()