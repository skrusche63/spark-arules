package de.kp.spark.arules.util
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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import de.kp.core.arules.{TopKNRAlgorithm,RuleG,Transaction,Vertical}

import java.util.{BitSet,Collections,Comparator}
import scala.collection.JavaConversions._

object VerticalBuilder {

  def build(sc:SparkContext,input:String):Vertical = {
    
    /**
     * STEP #1
     * 
     * Read data from file system
     */
    val file = textfile(sc,input).cache()
    
    /**
     * STEP #2
     * 
     * Determine max item
     */
    val ids = file.flatMap(value => value._2.map(item => Integer.parseInt(item))).collect()
    val max = sc.broadcast(ids.max)
    
    /**
     * STEP #3
     * 
     * Build transactions
     */  
    val transactions = file.map(valu => {
      
      val (tid,items) = valu
      val trans = new Transaction(items.length)
      
      trans.setId(tid.toString)      
      items.foreach(item => {
        
        if (item != "") {
          trans.addItem(Integer.parseInt(item)) 
        }
        
      })
		
      /*
       * Sort transactions by descending order of items because
       * TopKRules and TNR assume that items are sorted by lexical 
       * order for optimization
       */ 
	  Collections.sort(trans.getItems(), new Comparator[Integer](){
		def compare(o1:Integer, o2:Integer):Int = {
		  return o2-o1
	    }}
	  )
      
	  trans
	  
    })
    
    val total = sc.broadcast(transactions.count())

    def seqOp(vert:Vertical, trans:Transaction):Vertical = {
      
      vert.setSize(max.value)
     
      val tid   = trans.getId()
      val items = trans.getItems()
        
      for (item <- items) yield {
          
        val ids = vert.tableItemTids(item)
        if (ids == null) vert.tableItemTids(item) = new BitSet(total.value.toInt)
        
        /*
         * Associate items with respective transaction
         * using the transaction identifier tid
         */
        vert.tableItemTids(item).set(tid.toInt)
        /*
         * Update the support of this item
         */
        vert.tableItemCount(item) = vert.tableItemCount(item) + 1

      }

      vert.setTrans(trans)      
      vert
      
    }
    /*
     * Note that vert1 is always NULL
     */
    def combOp(vert1:Vertical,vert2:Vertical):Vertical = vert2      

    transactions.coalesce(1, false).aggregate(new Vertical())(seqOp,combOp)    

  }
    
  private def textfile(sc:SparkContext, input:String):RDD[(Int,Array[String])] = {
    
    sc.textFile(input).map(valu => {
      
      val parts = valu.split(",")  
      (parts(0).toInt,parts(1).split(" "))
    
    })

  }
  
}