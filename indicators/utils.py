from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()
bucket = 'indicators'

client = InfluxDBClient.from_env_properties(enable_gzip=True)

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

p = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)

# write_api.write(bucket=bucket, record=p)

## using Table structure
tables = query_api.query(f'from(bucket:"{bucket}") |> range(start: -10m)')

for table in tables:
    print(table)
    for row in table.records:
        print (row.values)


## using csv library
csv_result = query_api.query_csv(f'from(bucket:"{bucket}") |> range(start: -10m)')
val_count = 0
for row in csv_result:
    for cell in row:
        val_count += 1

def push_influxdb():
    
    raise NotImplementedError()



