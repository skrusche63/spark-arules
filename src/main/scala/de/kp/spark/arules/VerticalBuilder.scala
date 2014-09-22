package de.kp.spark.arules
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

import de.kp.core.arules.{Transaction, Vertical}

import java.util.BitSet

import scala.collection.JavaConversions._
import scala.Array.canBuildFrom

/**
 * VerticalBuilder generates a vertical database representation, 
 * which is a prerequisite for using Top-K and Top-K non-redundant
 * algorithms
 */
object VerticalBuilder {

  private val useAggregate = false

  def build(@transient sc:SparkContext,input:String):Vertical = {
    /*
     * Read data from file system
     */
    val file = textfile(sc,input).cache()
    build(file)
    
  }
  
  def build(dataset:RDD[(Int,Array[Int])]):Vertical = {
    
    val sc = dataset.context
    
    /*
     * STEP #1
     * 
     * Determine max item
     */
    val ids = dataset.flatMap(value => value._2).collect()
    val max = sc.broadcast(ids.max)
    
    /*
     * STEP #2
     * 
     * Build transactions
     */  
    val transactions = dataset.map(valu => {
      
      val (tid,items) = valu
      /*
       * Sort items of a transaction by descending order because
       * TopKRules and TNR assume that items are sorted by lexical 
       * order for optimization
       */ 
      val reverse = items.toSeq.sorted.reverse
      val trans = new Transaction(items.length)
      
      trans.setId(tid.toString)      
      reverse.foreach(item => trans.addItem(item))
      
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

    if (useAggregate) {
      transactions.coalesce(1, false).aggregate(new Vertical())(seqOp,combOp)    
      
    } else {
      /*
       * Assign transaction identifier to the respective item
       */
      val vertical = transactions.flatMap(trans => trans.getItems().map(item => (item,trans.getId())))
        .groupBy(group => group._1).map(valu => {
      
          val (item,data) = valu
          /*
           * Associate item with respective transactions
           * through a BitSet
           */      
          val bitset = new BitSet(total.value.toInt)
          for ((item,tid) <- data) bitset.set(tid.toInt)

          val support = data.size
          (item,bitset,support)
      
       }).collect()
    
      val tableItemTids = Array.fill[BitSet](max.value + 1)(new BitSet(total.value.toInt))
      val tableItemCount = Array.fill[Int](max.value + 1)(0)
    
      vertical.foreach(valu => {
      
        val (item,bitset,support) = valu
      
        tableItemTids(item) = bitset
        tableItemCount(item) =  support
      
      })
   
      new Vertical(tableItemTids,tableItemCount,transactions.collect(),max.value)    
      
    }

  }
    
  private def textfile(sc:SparkContext, input:String):RDD[(Int,Array[Int])] = {
    
    sc.textFile(input).map(valu => {
      
      val Array(sid,sequence) = valu.split(",")  
      (sid.toInt,sequence.split(" ").map(_.toInt))
    
    })

  }
  
}