import pandas as pd
import numpy as np
import random
import time

# 创建用户ID列表和电影ID列表
user_ids = list(range(1, 1001))
movie_ids = list(range(1, 1001))

# 生成评分和时间数据
ratings = [random.randint(1, 5) for _ in range(100000)]
timestamps = [int(time.time() - random.randint(1, 1617235200)) for _ in range(100000)]

# 创建DataFrame
data = {
    'user_id': np.random.choice(user_ids, 100000),
    'movie_id': np.random.choice(movie_ids, 100000),
    'rating': ratings,
    'timestamp': timestamps
}

df = pd.DataFrame(data)

# 保存为CSV文件
df.to_csv('movie_ratings_100k_int_timestamp.csv', index=False)