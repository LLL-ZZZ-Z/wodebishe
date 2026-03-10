import requests
import re
import pandas as pd
import random
import time
from urllib.parse import quote
from DrissionPage import ChromiumPage
from DrissionPage.common import Actions

# -------------------------- 核心配置项（可自定义） --------------------------
KEY_WORD = "青秀山"  # 固定爬取关键词：青秀山
LIMIT_COUNT = 200  # 爬取笔记数量上限（可改50/200等）
DELAY_TIME = (3, 6)  # 每次请求随机延时3-6秒（防反爬，不要改短）
# ---------------------------------------------------------------------------

# 第一步：修正Edge浏览器路径配置语法（适配新版DrissionPage）
from DrissionPage import ChromiumOptions

co = ChromiumOptions()  # 先创建配置对象
edge_path = r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
co.set_browser_path(edge_path)  # 新版语法：通过配置对象设置路径

# 初始化Edge浏览器（传入配置对象）
browser = ChromiumPage(co)
ac = Actions(browser)
# 监听小红书搜索接口的数据包（核心：获取JSON原数据）
browser.listen.start('web/v1/search/notes')

# 请求头配置（适配Edge，无需修改）
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
    'Referer': 'https://www.xiaohongshu.com/',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br'
}

# 存储所有爬取的笔记数据
all_data = []
# 编码关键词，拼接小红书搜索链接
key_word_enc = quote(KEY_WORD)
search_url = f"https://www.xiaohongshu.com/search_result?keyword={key_word_enc}&source=web_explore_feed&type=51"


def get_note_detail(note_id, xsec_token):
    """提取单条笔记的详细文字数据（标题/内容/互动数）"""
    try:
        # 拼接笔记详情链接
        note_url = f'https://www.xiaohongshu.com/explore/{note_id}?xsec_token={xsec_token}&xsec_source=pc_search'
        # 发送请求获取笔记页面
        response = requests.get(note_url, headers=HEADERS, timeout=10)
        response.raise_for_status()  # 捕获HTTP请求错误
        html = response.text

        # 正则提取核心数据（容错处理，避免单条笔记出错中断）
        title = re.findall('<meta name="og:title" content="(.*?)"', html)
        title = title[0].strip() if title else "无标题"

        content = re.findall('<meta name="description" content="(.*?)"', html)
        content = content[0].strip() if content else "无内容"

        like = re.findall('<meta name="og:xhs:note_like" content="(.*?)"', html)
        like = like[0] if like else "0"

        collect = re.findall('<meta name="og:xhs:note_collect" content="(.*?)"', html)
        collect = collect[0] if collect else "0"

        comment = re.findall('<meta name="og:xhs:note_comment" content="(.*?)"', html)
        comment = comment[0] if comment else "0"

        author = re.findall('<meta name="og:xhs:user_nickname" content="(.*?)"', html)
        author = author[0].strip() if author else "未知博主"

        # 返回整理后的笔记数据
        return {
            "博主名": author,
            "笔记标题": title,
            "笔记内容": content,
            "点赞数": like,
            "收藏数": collect,
            "评论数": comment,
            "笔记链接": note_url
        }
    except Exception as e:
        print(f"提取笔记{note_id}失败：{str(e)[:50]}")
        return None


def main():
    """主爬取逻辑：监听数据包+下滑加载+提取数据"""
    print(f"========== 开始爬取【{KEY_WORD}】相关笔记 ==========")
    print(f"爬取上限：{LIMIT_COUNT}条 | 防反爬延时：{DELAY_TIME[0]}-{DELAY_TIME[1]}秒")
    print("=" * 50)

    # 打开小红书搜索页面
    browser.get(search_url)
    browser.set.window.max()  # 窗口最大化，避免元素遮挡

    # 第一次运行需扫码登录（30秒倒计时）
    print("请在弹出的Edge浏览器中打开小红书APP扫码登录（30秒内完成）...")
    for i in range(30, 0, -1):
        print(f'\r登录倒计时：{i}秒', end='')
        time.sleep(1)
    print('\n登录倒计时结束，开始爬取数据...\n')

    # 循环爬取直到达到数量上限
    while len(all_data) < LIMIT_COUNT:
        try:
            # 等待搜索接口数据包返回（超时15秒）
            res = browser.listen.wait(timeout=15)
            json_data = res.response.body
            items = json_data.get('data', {}).get('items', [])

            # 无新数据则下滑加载
            if not items:
                print("未获取到新笔记，下滑加载更多...")
                ac.scroll(delta_y=2000)  # 下滑2000像素
                time.sleep(random.uniform(*DELAY_TIME))
                continue

            # 遍历数据包中的笔记
            for item in items:
                if len(all_data) >= LIMIT_COUNT:
                    break
                note_id = item.get('id')
                xsec_token = item.get('xsec_token')
                if not note_id or not xsec_token:
                    continue

                # 提取单条笔记详情
                note_data = get_note_detail(note_id, xsec_token)
                if note_data:
                    all_data.append(note_data)
                    # 打印爬取进度
                    print(
                        f"已爬取 {len(all_data)}/{LIMIT_COUNT} 条 | 博主：{note_data['博主名'][:8]} | 标题：{note_data['笔记标题'][:20]}...")

            # 下滑加载更多数据
            ac.scroll(delta_y=2000)
            time.sleep(random.uniform(*DELAY_TIME))

        except Exception as e:
            print(f"爬取过程出错：{str(e)[:50]}，继续下滑重试...")
            ac.scroll(delta_y=2000)
            time.sleep(random.uniform(*DELAY_TIME))
            continue

    # 保存数据到Excel
    if all_data:
        # 生成带时间戳的文件名，避免重复
        file_name = f"小红书_青秀山笔记_{len(all_data)}条_{time.strftime('%Y%m%d%H%M%S')}.xlsx"
        # 转换为DataFrame并保存
        df = pd.DataFrame(all_data)
        df.to_excel(file_name, index=False, engine='openpyxl')
        print("\n" + "=" * 50)
        print(f"爬取完成！共获取 {len(all_data)} 条有效笔记")
        print(f"文件保存路径：{file_name}")
    else:
        print("\n爬取失败：未获取到任何有效笔记！")

    # 关闭Edge浏览器
    browser.quit()


if __name__ == '__main__':
    # 执行主爬取函数
    main()