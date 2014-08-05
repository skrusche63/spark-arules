## Top-K (Non Redundant) Association Rules with Spark

Association rule mining is a wide-spread technique to determine hidden interesting relations between items in large-scale 
transaction databases. This technique is often applied to data recorded by point-of-sale systems in supermarkets and is able 
to determine associations of the following kind:

> A customer who is willing to buy one or more products together is likely to also buy other items as well.

Association rules are used as a basis for decision making in promotional pricing, product placement and more. The application of 
such rules, however, is not restricted to market basket analysis and will be used in intrusion detection, web usage mining and other 
areas.

### Apache Spark


From the [Apache Spark](https://spark.apache.org/) website:

> Apache Spark is a fast and general engine for large-scale data processing and is up to 100x faster than Hadoop MR in memory.

The increasing number of associated projects, such as [Spark SQL](https://spark.apache.org/sql/) and [Spark Streaming](https://spark.apache.org/streaming/), enables Spark to become the future  Unified Data Insight Platform. With this perspective in mind, in this project we have integrated recently published Association Rule algorithms with Spark. This allows for a seamless usage of association rule mining either with batch or streaming data sources.
