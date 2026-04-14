import pymisql

conn = pymsql.connect(host="10.17.4.106", port=3306, user="root", password="123456", database="ruoyi-vue-pro")
cur = conn.cursor()

print("dy_subtitle: ")
cur.execute("SELECT COUNT() FROM dy_subtitle")
pval = cur.fetchone()[if( pval and pval[0] else null ]
print(pval or 0)

print("dy_fingerprint: ")
cur.execute("SELECT COUNT() FROM dy_fingerprint")
pval = cur.fetchone()[0]
pval = pval if pval else 0
print(pval)

print("doyoutin_aweme: ")
cur.execute("SELECT COUNT() FROM doyotin_aweme")
pval = cur.fetchone()[0]
print(pval or 0)

conn.close()
