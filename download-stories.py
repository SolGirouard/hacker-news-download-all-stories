import urllib.request, urllib.error, urllib.parse
import json
import datetime
import time
import pytz
import re
import pandas as pd
from pandas import DataFrame

timestring = lambda t: time.mktime(t.timetuple())
start = timestring(datetime.datetime(2013,1,1,0,0,0,0))
end = timestring(datetime.datetime(2014,1,1,0,0,0,0))

iterations = 306
df = DataFrame()
hitsPerPage = 1000
requested_keys = ["objectID","title","url","points","num_comments","author","created_at_i"]

for i in range(iterations):
   try:
      prefix = 'https://hn.algolia.com/api/v1/'
      query = 'search_by_date?tags=story&hitsPerPage=%s&numericFilters=created_at_i>%d,created_at_i<%d' % (hitsPerPage, start, end)
      url = prefix + query
      req = urllib.request.Request(url)
      response = urllib.request.urlopen(req)
      data = json.loads(response.read().decode("utf-8"))
      data = DataFrame(data["hits"])[requested_keys]
      df = df.append(data,ignore_index=True)
      end = data.created_at_i.min()
      print(i)
      time.sleep(3.6)

   except Exception as e:
      print(e)

df["title"] = df["title"].map(lambda x: x.replace(',','').replace('"',"'"))
df["created_at"] = df["created_at_i"].map(lambda x: datetime.datetime.fromtimestamp(int(x), tz=pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S'))
df["url-domain"] = df["url"].map(lambda x: urllib.parse.urlparse(x).hostname)

ordered_df = df[["objectID","title","url-domain","points","num_comments","author","created_at"]]
ordered_df.to_csv("hacker_news_stories.csv", encoding='utf-8', index=False)
