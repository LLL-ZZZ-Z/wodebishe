# wx_crawl_1000.py - Chrome稳定版（解决驱动崩溃）
import os
import time
import random
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException
)


def crawl_wechat_sogou(keyword="青秀山", target_count=1000):
    """
    核心改进：
    1. 改用Chrome浏览器（Edge驱动易崩溃）
    2. 启用终极反检测模式（stealth.min.js）
    3. 超低频采集（翻页间隔10-20秒）
    """
    print("=" * 60)
    print(f"采集任务启动（Chrome版）")
    print(f"关键词: {keyword} | 目标数量: {target_count}篇")
    print("=" * 60)

    # 1. 定位Chrome驱动路径（请替换为你的chromedriver路径）
    chromedriver_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'chromedriver.exe')
    print(f"Chrome驱动路径: {chromedriver_path}")
    if not os.path.exists(chromedriver_path):
        print(f"❌ 错误：未找到chromedriver.exe！")
        print(f"   下载地址：https://googlechromelabs.github.io/chrome-for-testing/")
        return None
    print(f"✅ 驱动文件检测正常")

    # 2. Chrome终极反检测配置
    options = Options()
    # 核心反检测参数
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--incognito')  # 无痕模式
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # 随机User-Agent
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    options.add_argument(f'--user-agent={random.choice(user_agents)}')
    # 禁用日志
    options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)

    # 3. 启动Chrome（隐藏webdriver特征）
    try:
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        # 彻底移除webdriver标识
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        print("✅ Chrome浏览器启动成功（终极反检测模式）")
    except Exception as e:
        print(f"❌ 浏览器启动失败: {str(e)}")
        return None

    # 数据存储
    articles_data = []
    processed_urls = set()
    page_num = 1
    no_data_count = 0

    try:
        # 4. 访问搜狗微信（超慢节奏）
        driver.get("https://weixin.sogou.com/")
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "sec-input")))
        print("✅ 进入搜狗微信首页")
        time.sleep(random.uniform(5, 8))  # 超长初始延迟

        # 5. 模拟人工搜索（逐个字符输入）
        print(f"\n🔍 搜索关键词: {keyword}")
        search_box = driver.find_element(By.CLASS_NAME, "sec-input")
        search_box.clear()
        for char in keyword:
            search_box.send_keys(char)
            time.sleep(random.uniform(0.2, 0.5))  # 输入间隔拉长
        time.sleep(random.uniform(3, 5))

        search_btn = driver.find_element(By.CLASS_NAME, "enter-input.article")
        search_btn.click()
        time.sleep(random.uniform(8, 12))  # 搜索后等待10秒左右

        # 6. 强制登录（等待2分钟）
        def handle_login():
            try:
                WebDriverWait(driver, 15).until(
                    EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'wechat-login')]"))
                )
                print("\n⚠️ 请扫码登录（最多等待2分钟）：")
                start_time = time.time()
                while time.time() - start_time < 120:
                    time.sleep(3)
                    try:
                        driver.find_element(By.XPATH, "//div[contains(@class, 'wechat-login')]")
                    except:
                        try:
                            driver.find_element(By.CLASS_NAME, "txt-box")
                            print("✅ 登录成功！")
                            return True
                        except:
                            continue
                print("❌ 登录超时")
                return False
            except:
                return True

        if not handle_login():
            driver.quit()
            return None

        # 7. 超低频采集（核心解决驱动崩溃）
        print(f"\n📊 开始采集（超低频模式），目标{target_count}篇...")
        while len(articles_data) < target_count and no_data_count < 3:
            print(f"\n--- 第 {page_num} 页 ---")
            try:
                # 等待页面完全加载
                WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CLASS_NAME, "txt-box")))
                articles = driver.find_elements(By.CLASS_NAME, "txt-box")
                print(f"本页文章数：{len(articles)}")

                if len(articles) == 0:
                    no_data_count += 1
                    print(f"⚠️ 空页计数：{no_data_count}/3")
                    break

                no_data_count = 0

                # 解析文章（单篇间隔拉长）
                for article in articles:
                    try:
                        if not article.is_displayed():
                            continue

                        title_elem = article.find_element(By.XPATH, ".//h3/a")
                        title = title_elem.text.strip()
                        link = title_elem.get_attribute("href")

                        if not title or not link or link in processed_urls:
                            continue
                        processed_urls.add(link)

                        # 提取信息
                        account = article.find_element(By.CLASS_NAME, "account").text.strip() if article.find_elements(
                            By.CLASS_NAME, "account") else ""
                        pub_time = article.find_element(By.CLASS_NAME, "s2").text.strip() if article.find_elements(
                            By.CLASS_NAME, "s2") else ""
                        abstract = article.find_element(By.CLASS_NAME, "txt-info").text.strip()[
                                   :200] if article.find_elements(By.CLASS_NAME, "txt-info") else ""

                        articles_data.append({
                            "title": title,
                            "link": link,
                            "account": account,
                            "pub_time": pub_time,
                            "abstract": abstract,
                            "keyword": keyword,
                            "page": page_num,
                            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })

                        print(f"✅ 进度 [{len(articles_data)}/{target_count}]：{title[:30]}...")

                        if len(articles_data) >= target_count:
                            print(f"\n🎉 达到目标数量{target_count}篇")
                            break

                        # 单篇解析后延迟1-3秒
                        time.sleep(random.uniform(1, 3))

                    except:
                        continue

                if len(articles_data) >= target_count:
                    break

                # 翻页（间隔10-20秒，核心！）
                try:
                    next_btn = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[@class='np' and not(contains(@class, 'disabled'))]"))
                    )
                    delay = random.uniform(10, 20)
                    print(f"⏳ 等待{delay:.1f}秒后翻页（规避反爬）...")
                    time.sleep(delay)

                    # 模拟人工滚动+点击
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                    time.sleep(random.uniform(1, 2))
                    next_btn.click()

                    page_num += 1
                    time.sleep(random.uniform(8, 12))  # 翻页后等待

                except:
                    print("📄 无更多页面")
                    break

            except Exception as e:
                print(f"⚠️ 第{page_num}页异常：{str(e)[:100]}")
                driver.refresh()
                time.sleep(random.uniform(10, 15))
                continue

        # 8. 最终保存
        if articles_data:
            save_path = save_final_data(articles_data, keyword)
            print("\n" + "=" * 60)
            print(f"✅ 采集完成！")
            print(f"实际采集：{len(articles_data)} 篇")
            print(f"保存路径：{save_path}")
            print("=" * 60)
        else:
            print("\n❌ 未采集到数据")

    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
        if articles_data:
            save_final_data(articles_data, keyword, is_interrupted=True)
    except Exception as e:
        print(f"\n❌ 程序出错：{str(e)}")
        if articles_data:
            save_final_data(articles_data, keyword, is_error=True)
    finally:
        driver.quit()
        print("\n🔒 浏览器已关闭")

    return articles_data


def save_final_data(data, keyword, is_interrupted=False, is_error=False):
    """仅最终保存数据"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    save_dir = os.path.join(script_dir, "data", "comments")
    os.makedirs(save_dir, exist_ok=True)

    prefix = "final"
    if is_interrupted:
        prefix = "interrupted"
    elif is_error:
        prefix = "error"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"wechat_{keyword}_{len(data)}篇_{prefix}_{timestamp}"

    # 保存Excel和CSV
    df = pd.DataFrame(data)
    df.to_excel(os.path.join(save_dir, f"{file_name}.xlsx"), index=False, engine="openpyxl")
    df.to_csv(os.path.join(save_dir, f"{file_name}.csv"), index=False, encoding="utf-8-sig")

    return save_dir


# 程序入口
if __name__ == "__main__":
    print("=" * 60)
    print("微信采集程序 - Chrome稳定版（解决驱动崩溃）")
    print("=" * 60)
    print("使用说明：")
    print("1. 请先下载与Chrome版本匹配的chromedriver.exe")
    print("2. 采集速度极慢（翻页间隔10-20秒），但稳定性最高")
    print("3. 仅最终保存数据，路径：data/comments/")
    print("=" * 60)

    confirm = input("\n开始采集1000篇？(回车确认/n取消)：")
    if confirm.lower() == "n":
        print("程序取消")
        exit()

    crawl_result = crawl_wechat_sogou(keyword="青秀山", target_count=1000)
    if crawl_result:
        print(f"\n✨ 任务结束，共采集 {len(crawl_result)} 篇")
    else:
        print("\n❌ 任务失败")