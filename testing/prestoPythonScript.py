import prestodb
import time

conn = prestodb.dbapi.connect(
    host = 'localhost',
    port = 8080,
    user = 'hadoop',
    catalog = 'hive',
    schema = 'dell'
)

curr = conn.cursor()
curr.execute('show tables')
#curr.execute('select count(*) from company_master_lookup')
print(f"Stats: {curr.stats}\n")
print(f'Desc: {curr.description}\n')
print(f'Warnings: {curr.warnings}\n')
print(f"Stats: {curr.stats}\n")
print(curr.fetchall()[:100])
print(f"Stats: {curr.stats}\n")
print(f"Stats: {curr.stats}\n")
print(f'Desc: {curr.description}\n')
print(f'Warnings: {curr.warnings}\n')

curr.close()