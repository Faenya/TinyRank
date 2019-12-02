import warc
import ujson as json
import io
import re
from urlparse import urlparse, urlunparse


f = warc.open("./data/data.warc.gz")

out_links = io.open("./data/output_links.txt", "w+", encoding='utf-8')
out_rank = io.open("./data/output_rank.txt", "w+", encoding='utf-8')
out_links_scala =io.open("./data/output_links_scala.txt", "w+", encoding='utf-8')

for record in f:
  if record['content-type'] != 'application/json':
    continue
  payload = record.payload.read()

  try:
    clean_url = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    page_info = json.loads(payload)
    page_url = page_info['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
    if clean_url.match(page_url) != None:
      ranks = page_url + "\t1\n"
      outbound_links = page_info['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
      outbound_urls = set(filter(None, [url['url'] for url in outbound_links]))

      links = page_url + "\t{ "
      links_scala = page_url + " "
      first_link = True
      for outbound_url in outbound_urls :
        if (clean_url.match(outbound_url) != None and first_link) :
          links = (links + "(" + outbound_url + ")")
          links_scala = (links_scala + " " + outbound_url)
          first_link = False
        elif (clean_url.match(outbound_url) != None) : 
          links = (links + ", (" + outbound_url + ")")
          links_scala = (links_scala + " " + outbound_url)
      links = links + " }\n"
      links_scala = links_scala + " .\n"
      out_links.write(links)
      out_links_scala.write(links_scala)
      out_rank.write(ranks)

  except (KeyError, UnicodeDecodeError):
    pass

out_links.close()
out_rank.close()
out_links_scala.close()
