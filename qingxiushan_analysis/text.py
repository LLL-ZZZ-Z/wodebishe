# test_basic.py：测试MySQL读取+停用词加载（适配现有数据入库状态）
import sys
import os

# 把项目根目录加入Python路径，避免导入报错
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入修改后的工具函数
from src.utils.db_utils import read_ugc_data, read_weather_data
from src.utils.common_utils import load_stopwords
from config.preprocess_config import STOPWORDS_PATH

if __name__ == "__main__":
    print("===== 测试1：从MySQL读取核心UGC数据（携程+小红书） =====")
    try:
        ugc_df = read_ugc_data()
        # 打印前2行+数据列名，确认读取成功
        print("UGC数据列名：", ugc_df.columns.tolist())
        print("数据前2行：")
        print(ugc_df[["content", "score", "like_num", "source"]].head(2))
    except Exception as e:
        print(f"读取UGC数据失败：{e}")

    print("\n===== 测试2：从MySQL读取天气数据 =====")
    try:
        weather_df = read_weather_data()
        print("天气数据前2行：")
        print(weather_df[["weather_date", "max_temp", "min_temp", "weather_status"]].head(2))
    except Exception as e:
        print(f"读取天气数据失败：{e}")

    print("\n===== 测试3：加载停用词库 =====")
    try:
        stopwords = load_stopwords(STOPWORDS_PATH)
        print(f"成功加载停用词共{len(stopwords)}个，前10个：", list(stopwords)[:10])
    except Exception as e:
        print(f"加载停用词失败：{e}")