package org.rsp

import akka.actor.Actor
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.engine.QueryEngineFactory
import com.hp.hpl.jena.query.QueryExecutionFactory
import org.apache.jena.riot.RDFDataMgr
import scala.concurrent.Future

class RspJoin extends Actor{
  lazy val ds=RDFDataMgr.loadDataset("file:///C:/Users/calbimon/data/lsd/rdf/4UT01_2005_8_24.n3")          
  import scala.concurrent.ExecutionContext.Implicits.global 
  def receive ={
    case t:Triple =>
      println("let's dance"+t)
      if (t.getPredicate.getURI=="http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#procedure"){
        println("bitoio")
        query(t.getObject.getURI)
        .map{r=>
          println("Got this "+r.getString )           
        }
      }
  }
  def query(sys:String)={        
    val q= s"""
      SELECT * WHERE{
        <$sys> <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#ID> ?id 
      }
      """
    Future{        
    println("querying")
    val qe=QueryExecutionFactory.create(q, ds)
    val rs=qe.execSelect
    rs.next().getLiteral("id")
    }
  }
}