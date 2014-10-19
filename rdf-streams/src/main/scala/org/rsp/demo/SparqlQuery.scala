package org.rsp.demo

import com.hp.hpl.jena.query._
import scala.concurrent.Future
import org.rsp.DemoData
import scala.concurrent.ExecutionContext.Implicits.global

object SparqlQuery extends DemoData{
  
  import concurrent.ExecutionContext.Implicits.global
  
  /**
   * Execute Async Sparql query and return a result
   */
  def query(qu:String,ds:Dataset)=Future{//Remove the 'Future' and will be sync execution!
    val rs=QueryExecutionFactory.create(qu, ds).execSelect
    val id=rs.next.getLiteral("id")
    Thread.sleep(200) // simulate long query delay
    println(id)
  }
  val q= s"""
    SELECT * WHERE{
      ?a <${sswObs}ID> ?id 
    }""" 
  
  def main(args:Array[String]):Unit={
    (1 to 20).foreach{i=>
      query(q,ds1)
    }
    //results will appear after this message.
    println("Finished")
    Thread.sleep(5000)
  }
}