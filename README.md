![Elasticworks.](https://raw.githubusercontent.com/skrusche63/spark-arules/master/images/predictiveworks.png)

**Predictiveworks.** is an open ensemble of predictive engines and has been made to cover a wide range of today's analytics requirements. **Predictiveworks.**  brings the power of predictive analytics to Elasticsearch.

## Reactive Association Analysis Engine

The Association Analysis Engine is one of the nine members of the open ensemble and is built to support association rule mining with a new and redefined 
mining algorithm. The approach overcomes the well-known "threshold problem" and makes it a lot easier to directly leverage the resulting content and product rules.

The use cases are endless and only restricted by imagination:

##### Audience discovery & targeting, Cross-sell Analytics, Recommendations, Smart Data Management and more for Elasticsearch and other data sources.

Association rule mining is a wide-spread technique to determine hidden interesting relations between items in large-scale 
transaction databases. This technique is often applied to data recorded by point-of-sale systems in supermarkets or ecommerce web sites and is able to determine associations of the following kind:

> A customer who is willing to buy one or more products together is likely to also buy other items as well.

Association rules are used as a basis for decision making in promotional pricing, product placement and more. The application of such rules, however, is not restricted to market basket analysis and will be used in intrusion detection, web usage mining and other areas: 

For example, we have made good experience applying association rules to search queries, where they are used to expand the query based on terms already in the query. 

> If a query contains 'cattle' and 'neurological disorder', then this query may be extended by 'bovine spongiform encephalopathy' using the rule [cattle, neurological disorder] -> [bovine spongiform encephalopathy].

The Association Analysis Engine implements effective algorithms to discovery the most relevant hidden relations in large-scale datasets thereby avoiding the well-known threshold problems.


The Association Analysis Engine supports rule discovery and also real-time recommendations based on association rules. The engine is implemented as a micro service on top of [Akka](http://akka.io) and [Spark](https://spark.apache.org/) and can be easily integrated in a reactive loose coupling environment.

---

### Examples

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

The goal of association rule mining is to discover all rules that have a support and confidence above user-defined thresholds, 
*minimum support* and *minimum confidence*. The challenge is choose the right thresholds with respect to the considered transaction database.

This is a major problem, as one usually has limited resources to analyze the mining results, and fine tuning of the thresholds is time-consuming job. The problem is especially associated with *minimum support*:

**Threshold is set too high**: 
This generates too few results and valuable information may be omitted.

**Threshold is set too low**: 
This can generate a huge amount of results, and the mining task may become very slow.

The dependency of association rule algorithms on *minimum support* makes it almost impossible to "automate" association rule mining or use it streaming data sources.

In 2012, [Philippe-Fournier Viger](http://www.philippe-fournier-viger.com/) redefined the problem of association mining as **Top-K Association Rule Mining**. The proposed algorithm only depends on the parameters *k*, the number of rules to be generated, and *minimum confidence*. For more information, continue to read [here](http://www.philippe-fournier-viger.com/spmf/top_k_non_redundant_association_rules.pdf).


We adapted Viger's original implementation and made his **Top-K** and **Top-K Non Redundant** algorithms available for Spark.

---

### Akka

Akka is a toolkit to build concurrent scalable applications, using the [Actor Model](http://en.wikipedia.org/wiki/Actor_model). Akka comes with a feature called *Akka Remoting*, which easily enables to setup 
a communication between software components in a peer-to-peer fashion.

Akka is leveraged in this software project to enable external software projects to interact with this Association Analysis engine. Besides external communication, Akka is also used to implement the internal 
interaction between the different functional building blocks of the engine:

* Administration
* Indexing & Tracking
* Training
* Retrieval 

---

### Data Sources

The Reactive Association Analysis Engine supports a rapidly increasing list of applicable data sources. Below is a list of data sources that are already supported:

* Cassandra,
* Elasticsearch,
* HBase,
* MongoDB,
* Parquent,

and JDBC database.

