import pymysql
conn = pymysql.connect(host="10.17.4.106", port=3306, user="root", password="123456", database="ruoyi-vue-pro")
cur = conn.cursor()
cur.execute("SELECT status, COUNT(*) as count FROM dy_subtitle GROUP BY status")
for row in cur.fetchall():
    print(row)
conn.close()
