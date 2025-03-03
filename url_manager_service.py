"""
负责种子 URL 的管理、待爬取 URL 的存储和分发
"""

import redis
from flask import Flask, request, jsonify

app = Flask(__name__)
redis_pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
redis_client = redis.Redis(connection_pool=redis_pool)

# 初始化种子 URL
SEED_URLS = [
    'https://sc.chinaz.com/tupian/meishitupian.html'
]
for i in range(2, 245):
    SEED_URLS.append(f'https://sc.chinaz.com/tupian/meishitupian_{i}.html')


def init_urls():
    for url in SEED_URLS:
        redis_client.sadd('pending_urls', url)


# 随机分发策略
def random_distribute(num_urls):
    return redis_client.spop('pending_urls', count=num_urls)


# 轮询分发策略（此处简化实现）
def round_robin_distribute(num_urls):
    return redis_client.spop('pending_urls', count=num_urls)


@app.route('/get_urls', methods=['POST'])
def get_urls():
    data = request.get_json()
    num_urls = data.get('num_urls', 1)
    strategy = data.get('strategy', 'random')
    if strategy == 'random':
        urls = random_distribute(num_urls)
    elif strategy == 'round_robin':
        urls = round_robin_distribute(num_urls)
    else:
        return jsonify({"error": "Unsupported distribution strategy"}), 400
    return jsonify({"urls": urls})


if __name__ == '__main__':
    init_urls()
    app.run(host='0.0.0.0', port=5000)