"""
提供数据查询接口，允许用户查询存储的数据
"""
import sqlite3
from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/query_data', methods=['GET'])
def query_data():
    keyword = request.args.get('keyword')
    if not keyword:
        return jsonify({"error": "Keyword is required"}), 400
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    c.execute("SELECT * FROM images_fts WHERE images_fts MATCH ?", (keyword,))
    results = c.fetchall()
    conn.close()
    return jsonify({"results": results})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)