package de.kp.spark.arules.io
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-ARULES project
* (https://github.com/skrusche63/spark-arules).
* 
* Spark-ARULES is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-ARULES is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-ARULES. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/
import org.elasticsearch.common.xcontent.XContentBuilder
  
object ElasticBuilderFactory {
  /*
   * Definition of common parameters for all indexing tasks
   */
  val TIMESTAMP_FIELD:String = "timestamp"

  /******************************************************************
   *                          RULE
   *****************************************************************/
  
  /*
   * The unique identifier of the mining task that created the
   * respective rules
   */
  val UID_FIELD:String = "uid"
  
  /*
   * This is a relative identifier with respect to the timestamp
   * to specify which antecendents refer to the same association
   * rule
   */
  val RULE_FIELD:String = "rule"
  /* 
   * Antecedent specifies a SINGLE item reference from the respective
   * association rule; i.e. a single association rule is mapped onto
   * a set of index entries. This mechanism ensures that association
   * rules may be directly used with respect to item related search  
   */
  val ANTECEDENT_FIELD:String = "antecedent"
  val CONSEQUENT_FIELD:String = "consequent"

  val SUPPORT_FIELD:String = "support"
  val CONFIDENCE_FIELD:String = "confidence"
  /*
   * Weight specifies the ratio of 1 and the total number of antencents 
   * in the discovered rule; as the antecedent field in the search index
   * holds a SINGLE item reference (this may differ from the respective
   * associatoion rule), we also provide this ratio as a score parameter
   * to rank antecedents with respect to their weight.
   */
  val WEIGHT_FIELD:String = "weight"

  /******************************************************************
   *                          ITEM
   *****************************************************************/

  val SITE_FIELD:String = "site"
  val USER_FIELD:String = "user"

  val GROUP_FIELD:String = "group"
  val ITEM_FIELD:String  = "item"

  def getBuilder(builder:String,mapping:String):XContentBuilder = {
    
    builder match {
      /*
       * Elasticsearch is used to track item-based events to provide
       * a transaction database within an Elasticsearch index
       */
      case "item" => new ElasticItemBuilder().createBuilder(mapping)
      /*
       * Elasticsearch is also used as a rule sink to enable seamless
       * usage of the association rule mining results
       */
      case "rule" => new ElasticRuleBuilder().createBuilder(mapping)
      
      case _ => null
      
    }
  
  }

}