"""
接收 URL 管理服务分发的 URL，进行网页数据爬取和解析
"""
import sqlite3
from flask import Flask, request, jsonify

app = Flask(__name__)


def init_db():
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS images
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 name TEXT,
                 url TEXT)''')
    c.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS images_fts USING fts5(name, words)''')
    conn.commit()
    conn.close()


@app.route('/store_data', methods=['POST'])
def store_data():
    data = request.get_json().get('data', [])
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    for item in data:
        c.execute("INSERT INTO images (name, url) VALUES (?,?)", (item['name'], item['url']))
        image_id = c.lastrowid
        words_str = ' '.join(item['words'])
        c.execute("INSERT INTO images_fts (rowid, name, words) VALUES (?,?,?)", (image_id, item['name'], words_str))
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5002)