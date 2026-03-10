import logging
import time
import csv
import math
import os
from requests import post
from requests.exceptions import RequestException

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 核心配置（青秀山携程景点信息）
QINGXIUSHAN_ID = 8810
TOTAL_COMMENTS = 18818  # 全部评论总数（来自截图）
PAGE_SIZE = 20  # 携程接口固定每页20条
COMMENT_URL = 'https://m.ctrip.com/restapi/soa2/13444/json/getCommentCollapseList'

# 请求头（保留你的Cookie，确保请求合法）
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0',
    'Cookie': 'GUID=09031176114224533973; nfes_isSupportWebP=1; UBT_VID=1772013270289.fe5467zLT7cs; Session=smartlinkcode=U162778757&smartlinklanguage=zh&SmartLinkKeyWord=&SmartLinkQuary=&SmartLinkHost=; MKT_CKID=1772013270600.t2g10.5mpn; _RGUID=679aaf08-a858-4e89-ad45-c787ef6eedb2; Hm_lvt_a8d6737197d542432f4ff4abc6e06384=1773035037; Hm_lpvt_a8d6737197d542432f4ff4abc6e06384=1773035037; HMACCOUNT=10D5D35BFD62F8B6; Union=OUID=&AllianceID=4902&SID=22921635&SourceID=&createtime=1773035037&Expires=1773639836628; MKT_OrderClick=ASID=490222921635&AID=4902&CSID=22921635&OUID=&CT=1773035036630&CURL=https%3A%2F%2Fwww.ctrip.com%2F%3Fallianceid%3D4902%26sid%3D22921635%26msclkid%3D0079c736ffdf1698a2a0ae98a1c7af5b%26keywordid%3D82327006001797&VAL={"pc_vid":"1772013270289.fe5467zLT7cs"}; MKT_Pagesource=PC; _bfa=1.1772013270289.fe5467zLT7cs.1.1773035036529.1773035044458.3.2.290510; _jzqco=%7C%7C%7C%7C1773035036852%7C1.746659359.1772013270599.1773035036644.1773035044741.1773035036644.1773035044741.0.0.0.8.8',
    'Content-Type': 'application/json',
    'Referer': f'https://you.ctrip.com/sight/nanning166/{QINGXIUSHAN_ID}.html',
    'Accept': 'application/json, text/plain, */*',
    'Origin': 'https://you.ctrip.com',
    'Sec-Ch-Ua': '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'X-Ctx-Ubt-Pageid': '290510',
    'X-Ctx-Ubt-Pvid': '2',
    'X-Ctx-Ubt-Sid': '3',
    'X-Ctx-Ubt-Vid': '1772013270289.fe5467zLT7cs',
    'X-Ctx-Wclient-Req': '9ae4ae2e0ee36fd21500f1e21bdab39d'
}


def get_request_data(page_index, star_type=0, sort_type=3):
    """
    生成请求体
    :param page_index: 页码
    :param star_type: 星级筛选（0=全部，1=差评，5=好评）
    :param sort_type: 排序类型（3=按时间排序）
    :return: 字典格式请求体
    """
    return {
        "arg": {
            "resourceId": QINGXIUSHAN_ID,
            "resourceType": 11,
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "sortType": sort_type,
            "commentTagId": "0",
            "collapseType": 1,
            "channelType": 7,
            "videoImageSize": "700_392",
            "starType": star_type  # 0=全部评论
        },
        "head": {
            "cid": "09031176114224533973",
            "ctok": "",
            "cver": "1.0",
            "lang": "01",
            "sid": "8888",
            "syscode": "09",
            "auth": None,
            "extension": [{"name": "protocal", "value": "https"}]
        },
        "contentType": "json"
    }


def parse_comment(comment):
    """解析单条评论的完整字段"""
    # 核心字段
    user_id = comment.get('userInfo', {}).get('userId', '未知')
    content = comment.get('content', '').replace('\n', '').strip()
    publish_time = comment.get('publishTypeTag', '未知')  # 发布时间（如"2026-03"）
    ip_location = comment.get('ipLocatedName', '未知')  # IP属地

    # 评分字段（景色/趣味/性价比）
    scores = comment.get('scores', [])
    scenery_score = 0.0
    fun_score = 0.0
    cost_score = 0.0
    for score in scores:
        score_name = score.get('name', '')
        score_value = score.get('score', 0.0)
        if score_name == '景色':
            scenery_score = score_value
        elif score_name == '趣味':
            fun_score = score_value
        elif score_name == '性价比':
            cost_score = score_value

    # 互动字段
    useful_count = comment.get('usefulCount', 0)  # 赞同数
    collect_cnt = comment.get('collectCnt', 0)  # 收藏数
    tourist_type = comment.get('touristTypeDisplay', '未知')  # 出行类型（如"家庭游"）
    star_rating = comment.get('starRating', 0)  # 评论星级（1=差，2=较差，3=一般，4=好，5=很好）

    # 补充分类标签（方便后续筛选）
    if star_rating == 1:
        comment_type = "差评"
    elif star_rating == 2:
        comment_type = "较差"
    elif star_rating == 3:
        comment_type = "中评"
    elif star_rating == 4:
        comment_type = "好评"
    elif star_rating == 5:
        comment_type = "很好"
    else:
        comment_type = "未知"

    return [
        user_id, content, publish_time, ip_location, scenery_score,
        fun_score, cost_score, useful_count, collect_cnt, tourist_type,
        star_rating, comment_type  # 新增评论类型标签
    ]


def crawl_all_comments():
    """爬取青秀山全部评论（按时间排序）"""
    # 防错校验
    if PAGE_SIZE <= 0:
        logger.error(f"PAGE_SIZE必须为正数，当前值：{PAGE_SIZE}")
        return 0
    if TOTAL_COMMENTS <= 0:
        logger.error(f"总评论数必须为正数，当前值：{TOTAL_COMMENTS}")
        return 0

    # 计算最大页数（适配全部评论）
    max_pages = math.ceil(TOTAL_COMMENTS / PAGE_SIZE) + 30  # 额外30页容错
    max_pages = min(max_pages, 1000)  # 兜底：最多爬1000页

    crawled_num = 0
    consecutive_empty_pages = 0
    max_consecutive_empty = 5  # 连续5页空才终止（适配大量数据）

    # 创建存储目录
    save_dir = 'data/comments'
    os.makedirs(save_dir, exist_ok=True)
    save_path = f'{save_dir}/qingxiushan_{QINGXIUSHAN_ID}_全部评论_按时间排序.csv'

    # 初始化CSV表头（新增评论类型标签）
    with open(save_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            '用户ID', '评论文本', '发布时间', 'IP属地', '景色评分',
            '趣味评分', '性价比评分', '赞同数', '收藏数', '出行类型',
            '评论星级', '评论类型'  # 新增列
        ])

    logger.info(f"===== 开始爬取青秀山全部评论（总计{TOTAL_COMMENTS}条） =====")
    logger.info(f"预计爬取页数：{max_pages}页 | 每页{PAGE_SIZE}条")

    # 分页爬取核心逻辑
    for page in range(1, max_pages + 1):
        if crawled_num >= TOTAL_COMMENTS:
            logger.info(f"已爬取{crawled_num}条，达到总评论数{TOTAL_COMMENTS}，停止爬取")
            break

        # 生成请求体（star_type=0表示全部评论）
        request_data = get_request_data(page, star_type=0)
        try:
            # 发送请求（超时时间延长至20秒）
            response = post(
                url=COMMENT_URL,
                json=request_data,
                headers=HEADERS,
                timeout=20
            )
            response.raise_for_status()  # 触发HTTP错误（如403/500）
            result = response.json()

            # 解析评论列表
            comments = result.get('result', {}).get('items', [])
            if not comments:
                consecutive_empty_pages += 1
                logger.warning(f"第{page}页无数据，连续空页：{consecutive_empty_pages}/{max_consecutive_empty}")
                if consecutive_empty_pages >= max_consecutive_empty:
                    logger.info(f"连续{max_consecutive_empty}页无数据，爬取终止")
                    break
                time.sleep(3)  # 空页后延长等待
                continue

            # 重置空页计数
            consecutive_empty_pages = 0

            # 处理当前页评论
            page_comments = []
            for comment in comments:
                parsed_comment = parse_comment(comment)
                # 过滤空评论文本
                if not parsed_comment[1]:
                    continue
                page_comments.append(parsed_comment)
                crawled_num += 1

                # 达到总评论数则停止
                if crawled_num >= TOTAL_COMMENTS:
                    break

            # 写入CSV（追加模式）
            with open(save_path, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(page_comments)

            # 日志输出进度
            progress = (crawled_num / TOTAL_COMMENTS) * 100
            logger.info(f"第{page}页完成 | 累计{crawled_num}/{TOTAL_COMMENTS}条 | 进度：{progress:.1f}%")

            # 防反爬：动态间隔（2-6秒，页数越多间隔越长）
            sleep_time = 2 + min(page % 5, 4)  # 最大间隔6秒
            time.sleep(sleep_time)

        except RequestException as e:
            logger.error(f"第{page}页请求失败：{str(e)[:100]}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                logger.info(f"连续{max_consecutive_empty}页请求失败，爬取终止")
                break
            time.sleep(5)  # 请求失败后延长等待
            continue
        except Exception as e:
            logger.error(f"第{page}页解析失败：{str(e)[:100]}")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= max_consecutive_empty:
                logger.info(f"连续{max_consecutive_empty}页解析失败，爬取终止")
                break
            time.sleep(3)
            continue

    # 最终统计
    logger.info("\n" + "=" * 60)
    logger.info(f"爬取完成！实际获取 {crawled_num} 条评论")
    logger.info(f"数据文件路径：{save_path}")
    logger.info(f"未爬取原因：{'达到总评论数' if crawled_num >= TOTAL_COMMENTS else '接口无更多数据/请求失败'}")
    logger.info("=" * 60)

    return crawled_num


if __name__ == '__main__':
    # 执行全部评论爬取
    total_crawled = crawl_all_comments()

    # 输出最终结果
    if total_crawled > 0:
        print(f"\n✨ 任务完成！共爬取 {total_crawled} 条青秀山评论")
        print(f"📁 数据文件：data/comments/qingxiushan_8810_全部评论_按时间排序.csv")
    else:
        print("\n❌ 任务失败！未  爬取到任何评论，请检查Cookie或网络")