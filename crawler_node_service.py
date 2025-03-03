"""
接收爬虫节点解析后的数据，进行存储和索引
"""
import requests
import urllib.request
import random
from lxml import etree
import jieba
import multiprocessing
import time

# 随机 IP 代理库
PROXY_POOL = [
    {'http': '223.94.85.131:9091'},
    {'http': '217.60.194.52:8080'},
    {'http': '79.122.202.21:8080'},
    {'http': '18.130.252.7:8888'},
    {'http': '36.94.174.243:8080'},
    {'http': '103.119.67.41:3125'},
]


def get_random_proxy():
    return random.choice(PROXY_POOL)


def fetch_url(url):
    headers = {
        'User-Agent': random.choice([
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1'
        ])
    }
    request = urllib.request.Request(url=url, headers=headers)
    proxy = get_random_proxy()
    handler = urllib.request.ProxyHandler(proxy)
    opener = urllib.request.build_opener(handler)
    try:
        response = opener.open(request)
        return response.read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def parse_html(html):
    if not html:
        return []
    tree = etree.HTML(html)
    name_list = tree.xpath('//div[@class="item"]/img/@alt')
    src_list = tree.xpath('//div[@class="item"]/img/@data-original')
    parsed_data = []
    for name, src in zip(name_list, src_list):
        url = 'https:' + src
        words = jieba.lcut(name)
        parsed_data.append({
            'name': name,
            'url': url,
            'words': words
        })
    return parsed_data


def worker():
    url_manager_url = 'http://127.0.0.1:5000/get_urls'
    data_storage_url = 'http://127.0.0.1:5002/store_data'
    while True:
        try:
            response = requests.post(url_manager_url, json={"num_urls": 10, "strategy": "random"})
            if response.status_code == 200:
                urls = response.json().get('urls', [])
                if not urls:
                    break
                for url in urls:
                    html = fetch_url(url)
                    parsed_data = parse_html(html)
                    requests.post(data_storage_url, json={"data": parsed_data})
        except Exception as e:
            print(f"Error in worker: {e}")
            time.sleep(5)


if __name__ == '__main__':
    num_workers = multiprocessing.cpu_count()
    processes = []
    for _ in range(num_workers):
        p = multiprocessing.Process(target=worker)
        p.start()
        processes.append(p)
    for p in processes:
        p.join()