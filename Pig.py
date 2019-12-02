#!/usr/bin/python

from org.apache.pig.scripting import Pig
import time

P = Pig.compile("""

InOut = LOAD '$in_links' using PigStorage('\t') as (home_url: chararray, links:{ link: ( url: chararray ) } );

InPagerank = LOAD '$in_pagerank' using PigStorage('\t') as (home_url: chararray, rank : float);

InData = JOIN InPagerank by home_url, InOut by home_url;

Data = FOREACH InData GENERATE InOut::home_url as url, InPagerank::rank as rank, InOut::links as links;

outbound_pagerank = FOREACH Data GENERATE rank/COUNT(links) AS pagerank_transfer, FLATTEN (links) AS outbound_links;

new_pagerank = FOREACH (GROUP outbound_pagerank BY outbound_links) GENERATE group AS url, 0.15 + 0.85 * SUM(outbound_pagerank.pagerank_transfer) AS pagerank;

STORE new_pagerank INTO '$out' USING PigStorage('\t');

""")

params = {'in_links': './data/output_links.txt', 'in_pagerank': './data/output_rank.txt'}
times = []

for i in range(30):
   print("Iteration " + str(i))
   out = "PigResults/pagerank_data_" + str(i + 1)
   params['out'] = out
   tic = time.clock()
   stats = P.bind(params).runSingle()
   params['in_pagerank'] = out
   toc = time.clock()
   times.append(toc-tic)

print(times)
 
