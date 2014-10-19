package org.rsp

import org.apache.jena.riot.RDFDataMgr

trait DemoData {
  lazy val ds1=RDFDataMgr.loadDataset("4UT01_2005_8_24.n3")          
  lazy val ds2=RDFDataMgr.loadDataset("3CLO3_2005_8_29.n3")
  lazy val ds3=RDFDataMgr.loadDataset("690_2005_8_29.n3")
  
  val sswObs="http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#"
  val rspEx="http://w3c.org/rsp/example#"  
    
  def elapsed[R](block: => R): R = {
    val t0 = System.nanoTime
    val result = block    // call-by-name
    val t1 = System.nanoTime
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    result
  }

}