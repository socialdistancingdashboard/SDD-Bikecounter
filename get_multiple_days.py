from get_data_daily import *
from datetime import datetime, timedelta

print("This helper script downloads bike data for several days back and uploads them into S3 storage.")
daysback = int(input("How many days back do you want to get data? > "))
print(f"Going back {daysback} days...")
for days in range(daysback):
    query_date = datetime.today() - timedelta(days+1)
    print(f"\n---------------\n{query_date}\n---------------")
    data_json = get_data(query_date)
    write_data_to_s3(data_json, query_date)
