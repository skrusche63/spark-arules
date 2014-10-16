![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

## Reactive Association Analysis Engine

##### Audience discovery & targeting, Cross-sell Analytics, Recommendations, Smart Data Management and more for Elasticsearch and other data sources.

![Association Analysis Engine Overview](https://raw.githubusercontent.com/skrusche63/spark-arules/master/images/association-rules-overview.png)

Association rule mining is a wide-spread technique to determine hidden interesting relations between items in large-scale 
transaction databases. This technique is often applied to data recorded by point-of-sale systems in supermarkets or ecommerce web sites and is able to determine associations of the following kind:

> A customer who is willing to buy one or more products together is likely to also buy other items as well.

Association rules are used as a basis for decision making in promotional pricing, product placement and more. The application of such rules, however, is not restricted to market basket analysis and will be used in intrusion detection, web usage mining and other areas: 

For example, we have made good experience applying association rules to search queries, where they are used to expand the query based on terms already in the query. 

> If a query contains 'cattle' and 'neurological disorder', then this query may be extended by 'bovine spongiform encephalopathy' using the rule [cattle, neurological disorder] -> [bovine spongiform encephalopathy].

The Association Analysis Engine implements effective algorithms to discovery the most relevant hidden relations in large-scale datasets thereby avoiding the well-known threshold problems.


The Association Analysis Engine supports rule discovery and also real-time recommendations based on association rules. The engine is implemented as a micro service on top of [Akka](http://akka.io) and [Spark](https://spark.apache.org/) and can be easily integrated in a reactive loose coupling environment.

---

### Use Cases

Association Rule Mining is a wide-spread method that is used in many application areas. Here we describe selected use case that are relevant for ecommerce sites, and we certainly do not claim that the list of use cases is complete.

##### Data Management

Customer, product or content profiles and their relations describe a more or less decaying data perspective that reflect built-time information requirements. Valuable data associations that emerge during the life-time of a database backed system remain out of scope.

Association Rule Mining in large-scale tables even across databases help detects these hidden associations and helps to make data management more adaptive and responsive to data reality.

##### Discounting & Pricing

Discounting & promotional pricing has to ensure that the right discounting is offered to the right product. Knowledge about products that are often purchased together is important here: It helps to avoid simultaneous discounting of products that are in any case often sold together. It also helps to decide which products have to be discouunted to push sales of products.

The knowledge about products that are frequently purchased together also help to decide which other product can be offered as a bonus to customers that bought another product, e.g. to increase customer satisfaction.

##### Dynamic Product Catalogs

Knowing products (or categories) that are frequently purchased together helps to dynamically adapt product catalogs to actual behavior and needs of customers by simply grouping these products on a single catalog page.

##### Product Recommendation

Detecting product bundles in the customers previous purchase behavior is an imported information to increase cross-selling, and supports features such as "customer who have bought the product a certain customer is actually looking at also bought these products".

##### Purchase Prediction

Discovering products that are frequently bought together helps to predict which products will be probably bought next.

---

### Top-K (Non Redundant) Association Rules

Finding interesting associations between items in transaction databases is a fundamental data mining task. Finding association rules is 
usually accompanied by the following control parameters:

* **Support**: The percentage of transactions of the database where the rules occurs.

* **Confidence**: The support of the rule divided by the support of its *antecedent*.

The goal of association rule mining then is to discover all rules that have a support and confidence that is higher to user-defined thresholds 
*minimum support* and *minimum confidence*. The challenge is choose the right thresholds with respect to the considered transaction database.

This is a major problem, as one usually has limited resources for analyzing the mining results, and fine tuning of the thresholds is time-consuming job. The problem is especially associated with *minimum support*:

**Threshold is set too high**: 
This generates too few results and valuable information may be omitted.

**Threshold is set too low**: 
This can generate a huge amount of results, and the mining task may become very slow.

The dependency of association rule algorithms on *minimum confidence* makes it almost impossible to "automate" association rule mining or use it streaming data sources.

In 2012, [Philippe-Fournier Viger](http://www.philippe-fournier-viger.com/) redefined the problem of association mining as **Top-K Association Rule Mining**. The proposed algorithm only depends on the parameters *k*, the number of rules to be generated, and *minimum confidence*. For more information, continue to read [here](http://www.philippe-fournier-viger.com/spmf/top_k_non_redundant_association_rules.pdf).


We adapted Viger's original implementation and made his **Top-K** and **Top-K Non Redundant** algorithms available for Spark.

---

### Akka

Akka is a toolkit to build concurrent scalable applications, using the [Actor Model](http://en.wikipedia.org/wiki/Actor_model). Akka comes with a feature called *Akka Remoting*, which easily enables to setup a communication between software components in a peer-to-peer fashion.

Akka and Akka Remoting are an appropriate means to establish a communication between prior independent software components - easy and fast.

---

### Spark

From the [Apache Spark](https://spark.apache.org/) website:

> Apache Spark is a fast and general engine for large-scale data processing and is up to 100x faster than Hadoop MR in memory.

The increasing number of associated projects, such as [Spark SQL](https://spark.apache.org/sql/) and [Spark Streaming](https://spark.apache.org/streaming/), enables Spark to become the future  Unified Data Insight Platform. With this perspective in mind, we have integrated recently published Association Rule algorithms with Spark. This allows for a seamless usage of association rule mining either with batch or streaming data sources.

---

### Data Sources

The Reactive Association Analysis Engine supports a rapidly increasing list of applicable data sources to discover content, product, service or state rules. Content rules specify which e.g. articles from online publishers are often read or viewed together, whereas product and service rules provide valuable insights for retailers. States are short for customer states used to describe customer behavior, and state rules specify which frequent behavioral patterns customers show.

Below is a list of data sources that are already supported or will be supported in the near future:

#### Elasticsearch

[Elasticsearch](http://www.elasticsearch.org) is a flexible and powerful distributed real-time search and analytics engine. Besides linguistic and semantic enrichment, for data in a search index there is an increasing demand to apply analytics, knowledge discovery & data mining, and even predictive analytics to gain deeper insights into the data and further increase their business value.

A step towards analytics is the recently introduced combination with [Logstash](http://logstash.net/) to easily store logs and other time based event data from any system in a single place.

The Association Analysis Engine comes with a connector to Elasticsearch and thus brings knowledge discovery and data mining to the world of indexed data. The use cases are endless. 

E.g. Elasticsearch may be used to support product search for an ecommerce platform and also as a NoSQL database to store order and cart events. Connected to the Association Analysis Engine, Elasticsearch also turns into a Market Basket Analysis, and Real-time Recommendation platform. 

#### Piwik Analytics

[Piwik Analytics](http://piwik.org) is the leading and widely used open source web analytics platform, and is an excellent starting point to move into the world of dynamic catalogs, product recommendations, purchase predictions and more.

#### Pimcore (coming soon)

[Pimcore](http://pimcore.org) is an open source multi-channel experience and engagement management platform and contains a variety of integrated applications such as Digital Asset Management, Ecommerce Framework, Marketing & Campaign Management, Multi-Channel & Web-to-Print, Product Information Management, Targeting & Personalization and
Web Content Management.

#### Relational Databases

Elasticsearch is one of the connectors actually supported. As many ecommerce sites and analytics platforms work with JDBC databases, the Association Analysis Engine also comes with a JDBC connector.

![Association Rules in Relational Databases](https://raw.githubusercontent.com/skrusche63/spark-arules/master/images/association-rules.png)

Discovering hidden relations in multiple large-scale tables even across databases is an emerging data management task. Database structures are usually pre-defined and reflect the information requirements that were
valid when the respective system was built. In other words, tables and their relations specify a more or less decaying data perspective. And, real-world data often have valuable associations that were simply out of scope when the respective data model was built.

Association Rule Mining in relational database tables is an appropriate means to detect overlooked data relations and thereby helps to make data management or adaptive to the data reality.

---

### Data Sinks

#### Elasticsearch

The Association Analysis Engine writes discovered rules to an Elasticsearch index. This ensures that these rules e.g. may directly be used for product recommendations delivered with appropriate product search results.

#### Redis

[Redis](http://redis.io) is open source and an advanced key-value cache and store, often referred to as a distributed data structure server. The Association Analysis Engine writes discovered rules to a Redis instance as a multi-purpose serving layer for software enrichments that are not equipped with Elasticsearch.

---

### Technology

* Akka
* Akka Remoting
* Elasticsearch
* Spark
* Spark Streaming
* Spray

