import trino
import time

conn = trino.dbapi.connect(
    host = 'localhost',
    port = 8080,
    user = 'hadoop',
    catalog = 'hive',
    schema = '6sense',
    client_tags = ['infratrino']
)

curr = conn.cursor()
#curr.execute('show teebles')
curr.execute('show tables')
print(f"Stats: {curr.stats}\n")
print(f'Desc: {curr.description}\n')
print(f'Warnings: {curr.warnings}\n')
print(f"Stats: {curr.stats}\n")
print(curr.fetchall()[0:100])
print(f"Stats: {curr.stats}\n")
print(f"Stats: {curr.stats}\n")
print(f'Desc: {curr.description}\n')
print(f'Warnings: {curr.warnings}\n')

curr.close()