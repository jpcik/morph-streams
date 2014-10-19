package org.rsp

import com.hp.hpl.jena.rdf.model.ModelFactory
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.rdf.model.ResourceFactory
import scala.collection.JavaConversions._
import com.hp.hpl.jena.sparql.core.DatasetGraph
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory
import com.hp.hpl.jena.graph.Graph
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.sparql.graph.GraphFactory
import com.hp.hpl.jena.sparql.core.Quad
import org.apache.jena.riot.RDFFormat
import java.io.FileWriter
import scala.collection.mutable.ArrayBuffer

object ObservationGrouping {
  val owlTime="http://www.w3.org/2006/time#" 
  val omOwl="http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#"
  val timeInstant=ResourceFactory.createResource(owlTime+"Instant")
  val samplingTime=ResourceFactory.createProperty(omOwl+"samplingTime")
  val result=ResourceFactory.createProperty(omOwl+"result")

  def group={
    val ds=RDFDataMgr.loadDataset("file:///C:/Users/calbimon/data/lsd/4UT01_2004_8_10.n3", RDFLanguages.N3)
    println("dippy")
    val fw=new FileWriter("demo.trig") 
    val m=ds.getDefaultModel
    val instants=m.listSubjectsWithProperty(RDF.`type`,timeInstant).toSeq
    val ordered = instants.map{ins=>
      val nn=ins.getLocalName.substring(8).split("_").map{n=>
        if (n.length==1) "0"+n
        else n
      }.reduce((a,b)=>a+b).toLong
      (nn,ins)
    }sortBy(_._1)
    ordered.foreach{case(n,ins)=>
      val dg=DatasetGraphFactory.createMem()    
      val g=NodeFactory.createURI(ins.toString)
      ins.listProperties.foreach{stm=>
        dg.add(new Quad(g,stm.asTriple))
      }    
      m.listSubjectsWithProperty(samplingTime, ins).foreach{obs=>
        obs.listProperties.foreach{stm=>
          dg.add(new Quad(g,stm.asTriple))
          if (stm.getPredicate.getURI.equals(result.getURI)){
            stm.getObject.asResource.listProperties.foreach{stm2=>
              dg.add(new Quad(g,stm2.asTriple))
            }          
          }
        }      
      }
      RDFDataMgr.write(fw,dg,RDFFormat.TRIG)
    }
    fw.close
  }
  
  def main(args:Array[String])={
    group
  }
  
  
  
}