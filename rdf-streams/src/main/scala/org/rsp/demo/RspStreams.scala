package org.rsp.demo

import com.hp.hpl.jena.graph.Factory
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory
import com.hp.hpl.jena.sparql.core.Quad
import com.hp.hpl.jena.vocabulary.RDF
import org.rsp.jena.JenaPlus._
import org.rsp.DemoData
import org.apache.jena.riot.lang.PipedRDFIterator
import org.apache.jena.riot.lang.PipedTriplesStream
import org.apache.jena.riot.RDFDataMgr
import scala.concurrent.Future
import collection.JavaConversions._
import scala.util.Random

/**
 * RDF streams examples using Jena
 */
object RspStreams extends DemoData{

  /**
   * This is a an RDF bounded sequence of triples
   */
  def sequenceTriples(num:Int)={
    (1 to num) map{i=>      
      new Triple(rspEx+i,RDF.`type`,sswObs+"Measurement")
    }
  }
  
  /**
   * This is an infinite stream of triples
   */
  def streamTriples={
    Iterator.from(10) map{i=>
      Thread.sleep(1000)
      new Triple(rspEx+i,sswObs+"observedValue",Random.nextDouble)      
    } 
  }
  
  /**
   * This is an infinite stream of quads
   */
  def streamQuads={
    val graphUri=rspEx+"graph1"
    streamTriples.map{tri=>
      new Quad(graphUri,tri)
    }
  }
  
  /**
   * This is an infinite stream of graphs
   */
  def streamGraphs={
    Stream.from(1) map{i=>
      val graph=Factory.createGraphMem
      sequenceTriples(5).foreach{t=>
        graph.add(t)
      }
      graph
    }
  }
 
  import scala.concurrent.ExecutionContext.Implicits.global
  
  /**
   * Get iterator stream of triples from file
   */
  def triplesFromFile={
    val iter= new PipedRDFIterator[Triple]
    Future { 
      RDFDataMgr.parse(new PipedTriplesStream(iter),"4UT01_2004_8_10.n3")      
    }
    Future(iter)
  }
  
  def main(args:Array[String]):Unit={
    //sequenceTriples.foreach(println)
    //streamTriples.foreach(println) 
    //streamQuads.foreach(println)
    //streamGraphs.foreach(println)
    triplesFromFile.map(i=>i foreach(println))
    println("Execute next...")
    Thread.sleep(5000)
  }
}