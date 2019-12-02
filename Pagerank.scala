import java.io._

val tic = System.currentTimeMillis()

val Links = sc.textFile("./data/output_links_scala.txt").map(line => (line.split(" ", 2)(0), line.split(" ", 2)(1).replace(" .", "").split(" ")))

var ranks = Links.mapValues(link => 1.0)

for (i <- 1 to 30) {

	println("Iteration " + i)

	val outbound_pagerank = Links.join(ranks).flatMap{case (homepage_url, (urls, rank)) => urls.map(url => (url, rank/urls.size))}

	ranks = outbound_pagerank.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
	
}

ranks.saveAsTextFile("SparkOutput")

val toc = System.currentTimeMillis()

println(toc - tic +" ms")