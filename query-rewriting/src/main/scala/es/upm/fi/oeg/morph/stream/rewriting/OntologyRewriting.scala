package es.upm.fi.oeg.morph.stream.rewriting

import org.oxford.comlab.requiem.rewriter.TermFactory
import es.upm.fi.oeg.sparqlstream.StreamQuery
import org.slf4j.LoggerFactory

object OntologyRewriting {
  
  private val tf=new TermFactory
  private val logger=LoggerFactory.getLogger(this.getClass)

  def translate(query:StreamQuery,ontology:String)={
    val k=new Kyrie(ontology)
   
    val clausifier=new SparqlClausifier(tf,query)
   
    val clauseQuery=clausifier.clausifiedQuery
    logger.debug("The clausified query: "+clauseQuery)
    val fclauses=k.rewriteDatalogClauses(Seq(clauseQuery))
    clausifier.sparqlizeUCQ(fclauses)
 
    logger.debug("Expanded query: "+query.toString)
    val reordered=QueryReordering.reorder(query)
        logger.debug("reordered query: "+reordered.toString)
    reordered
    //super.translate(reordered)
  }

}