import random
import string

# 生成随机的网址
def generate_url():
    letters = string.ascii_lowercase
    url = "https://www."
    for i in range(random.randint(4, 12)):
        url += random.choice(letters)
    url += ".com"
    print(url)
    return url

# 生成网址日志数据集
def generate_logs(n):
    logs = []
    for i in range(n):
        ip_address = ".".join(str(random.randint(0, 255)) for _ in range(4))
        user_id = random.randint(1000, 9999)
        url = generate_url()
        logs.append(f"{ip_address} - user_{user_id} [{random.randint(1, 28)}/Feb/2024:16:00:00 +0800] \"GET {url} HTTP/1.1\" 200 12345")
    return logs

# 生成1000条网址日志数据集
logs = generate_logs(1000)
# 将数据集写入文件
with open("wenjian/1.txt", "w") as f:
    for log in logs:
        f.write(log + "\n")
