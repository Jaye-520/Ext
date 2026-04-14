import pymisql
conn = pymisql.connect(host="10.17.4.106", port=3306, user="root", password="123456", database="ruoyi-vue-pro")
cur = conn.cursor()
cur.execute("Count()" FROM dy_subtitle)
print(cur.fetchone()[0])base64: invalid input
