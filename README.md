# TinyRank

M2 project by Martin Ars and Alexis Claveau for the Large Scale Distributed Data Management class.

## Downloading and processing the data

### Downloading the data

The data used during this project can be found here : [source](https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-47/segments/1573496664437.49/wat/CC-MAIN-20191111191704-20191111215704-00000.warc.wat.gz)

The file must be saved in the data folder under the name data.warc.gz for the script to work.

### Processing the data

The data can be processed using the provided script (createInputsFromWARC.py). This script requires python2.7 (due to the urlparse library). It also requires the warc and the ujson librairies which can be downloaded through pip.

The script generates 3 files : output_links and output_ranks used with Apache Pig as well as output_links_scala used with Apache Spark. These files have been pre-processed and are already present in the project in the data folder.

## Apache Pig

Once the input files have been obtained, the Pig script can be run using the following command while being located at the root of the project :

`pig -x local Pig.py`

The results are located in the PigResults folder.

## Apache Spark

Once the input file has been obtained, the Scala script can be run through Spark using the following commands while being located at the root of the project :

Run `spark-shell` then once in, run `load: Pagerank.scala`

The results are located in the SparkOutput folder.
