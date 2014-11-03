package de.kp.spark.arules.sink
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

import java.util.{Date,UUID}
import java.sql._

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model._

class JdbcSink {

  protected val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  protected val (url,database,user,password) = Configuration.mysql

  def addRules(req:ServiceRequest,rules:Rules) {
    
    /* Create tables if not exist */
    createTables()
    /* Insert tables */
    insertRules(req,rules)
    
  }
  
  private def insertRules(req:ServiceRequest,rules:Rules) {
    
    var conn:Connection = null
    var stmt:Statement  = null
    
    try {
      
      conn = getConnection(url,database,user,password)
      stmt = conn.createStatement()
    
      /* Create timestamp for the actual set of rules */
      val now = new Date()
      val timestamp = now.getTime()
    
      for (rule <- rules.items) {
        /*  Add rule record */      
        val rid = UUID.randomUUID().toString()        
        val ruleSQL = "INSERT INTO " + database + ".predictiveworks_rules " +
                      "VALUES ('$1', '$2', $3, $4, $5)"
                      .replace("$1",rid)
                      .replace("$2",req.data("uid"))
                      .replace("$3",timestamp.toString)
                      .replace("$4",rule.support.toString)
                      .replace("$5",rule.confidence.toString)
        
        stmt.execute(ruleSQL)
         
        /* Add antecedent records */
        for (item <- rule.antecedent) {
        
          val aid = UUID.randomUUID().toString()        
          val antecedentSQL = "INSERT INTO " + database + ".predictiveworks_antecedent " +
                              "VALUES ('$1', '$2', $3)"
                              .replace("$1",aid)
                              .replace("$2",rid)
                              .replace("$3",item.toString)
        
          stmt.execute(antecedentSQL)
        
        }
        /* Add consequent records */
        for (item <- rule.consequent) {
        
          val cid = UUID.randomUUID().toString()        
          val consequentSQL = "INSERT INTO " + database + ".predictiveworks_consequent " +
                              "VALUES ('$1', '$2', $3)"
                              .replace("$1",cid)
                              .replace("$2",rid)
                              .replace("$3",item.toString)
        
          stmt.execute(consequentSQL)
        
        }
        
      }
      
    } catch {
      case t:Throwable => {
        
      }
      
    } finally {

      try {
        if (stmt != null) stmt.close()
      
      } catch{
        case t:Throwable => {/* do nothing */}
      
      }
      
      try {
         if(conn != null) conn.close()
         
      } catch {
        case t:Throwable => {/* do nothing */}

      }
      
    }
    
  }
  /**
   * This method adds 3 tables to an existing JDBC database; these tables
   * specify the 
   */
  private def createTables() {
    
    var conn:Connection = null
    var stmt:Statement  = null
    
    try {
      
      conn = getConnection(url,database,user,password)
      stmt = conn.createStatement()

      /*
       * Create rules table: this table specifies the timestamp, 
       * unique task identifier (uid) and confidence and suport
       * of the respective rule
       */
      val rulesSQL = "CREATE TABLE IF NOT EXISTS " + database + ".predictiveworks_rules " +
                     "(rid VARCHAR(255) not NULL, " +
                     " uid VARCHAR(255), " + 
                     " timestamp BIGINT, " + 
                     " support INTEGER, " + 
                     " confidence DOUBLE, " + 
                     " PRIMARY KEY ( rid ))"

      stmt.execute(rulesSQL)
      /*
       * Create antecedent table, which holds the relationship
       * between the respective rule and the antecedent item
       */
      val antecedentSQL = "CREATE TABLE IF NOT EXISTS " + database + ".predictiveworks_antecedent " +
                   "(aid VARCHAR(255) not NULL, " +
                   " rid VARCHAR(255), " + 
                   " antecedent INTEGER, " +
                   " PRIMARY KEY ( aid ), FOREIGN KEY ( rid ) REFERENCES " + database + ".predictiveworks_rules (rid))" 

      stmt.execute(antecedentSQL)
      /*
       * Create consequent table, which holds the relationship
       * between the respective rule and the consequent item
       */
      val consequentSQL = "CREATE TABLE IF NOT EXISTS " + database + ".predictiveworks_consequent " +
                   "(cid VARCHAR(255) not NULL, " +
                   " rid VARCHAR(255), " + 
                   " antecedent INTEGER, " +
                   " PRIMARY KEY ( cid ), FOREIGN KEY ( rid ) REFERENCES " + database + ".predictiveworks_rules (rid))" 

      stmt.execute(consequentSQL)
    
    } catch {
      case t:Throwable => {
        
      }
      
    } finally {

      try {
        if (stmt != null) stmt.close()
      
      } catch{
        case t:Throwable => {/* do nothing */}
      
      }
      
      try {
         if(conn != null) conn.close()
         
      } catch {
        case t:Throwable => {/* do nothing */}

      }
      
    }
  
  }
  protected def getConnection(url:String,database:String,user:String,password:String):Connection = {

    /* Create MySQL connection */
	Class.forName(MYSQL_DRIVER).newInstance()	
	val endpoint = getEndpoint(url,database)
    
	/* Generate database connection */
	val	connection = DriverManager.getConnection(endpoint,user,password)
    connection
    
  }
  
  protected def getEndpoint(url:String,database:String):String = {
		
	val endpoint = "jdbc:mysql://" + url + "/" + database
	endpoint
		
  }

}