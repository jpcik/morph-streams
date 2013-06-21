package es.upm.fi.oeg.morph.stream.rewriting

import java.util.Properties
import es.upm.fi.oeg.sparqlstream.SparqlStream
import es.upm.fi.oeg.sparqlstream.StreamQuery
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.kyrie.Kyrie
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import collection.JavaConversions._
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock
import com.hp.hpl.jena.sparql.core.TriplePath
import org.oxford.comlab.requiem.rewriter.Clause
import org.oxford.comlab.requiem.rewriter.Term
import org.oxford.comlab.requiem.rewriter.TermFactory
import org.oxford.comlab.requiem.rewriter.FunctionalTerm
import org.oxford.comlab.requiem.rewriter.Variable
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.sparql.core.PathBlock
import com.hp.hpl.jena.sparql.syntax.ElementUnion
import es.upm.fi.oeg.morph.voc.RDF
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.graph.Node_URI
import com.hp.hpl.jena.rdf.model.ResourceFactory

class ExpansionQueryRewriting(props:Properties,mapping:String) 
  extends QueryRewriting(props,mapping){
  private val logger=LoggerFactory.getLogger(this.getClass)
  private val tf=new TermFactory
  
  override def translate(query:StreamQuery)={
    val k=new Kyrie("src/test/resources/ontologies/sensordemo.owl")
    //val mappedVars=vars(query.getQueryPattern).toSet.zipWithIndex.toMap
    //val inverseVars=mappedVars.map(m=>"?"+m._2.toString->m._1)
    //val mappedVocab=vocab(query.getQueryPattern).map(v=>v.getLocalName->v).toMap
    
    /*val projVars=query.getProjectVars.map(v=>v.getVarName).toSeq.filter(v=>mappedVars.contains(v))
    val qclauses=clausifier.clausify(query.getQueryPattern,mappedVars).toArray
    val head=new FunctionalTerm("Q",projVars.map(v=>tf.getVariable(mappedVars(v))).toArray)
    val dtQuery=new Clause(qclauses,head)*/
    val clausifier=new SparqlClausifier(tf,query)
   
    val clauseQuery=clausifier.clausifiedQuery
    logger.debug("The clausified query: "+clauseQuery)
    val fclauses=k.rewriteDatalogClauses(Seq(clauseQuery))
    clausifier.sparqlizeUCQ(fclauses)
 
    super.translate(query)
  }
  
  /*
  private def vocab(el:Element):Seq[Node_URI]=el match {
    case group:ElementGroup=>group.getElements.map(vocab(_)).flatten
    case path:ElementPathBlock=>path.getPattern.map(t=>vocab(t)).toSeq.flatten
    case _ => Seq()
  }

  private def vocab(t:TriplePath)={
    Seq(t.getSubject match {case v:Node_URI=>v
                             case _ =>println("gogo"+t.getSubject.getClass); null  },
        t.getPredicate match {case  v:Node_URI=>v
                             case _ => println("gogoro"+t.getPredicate.getClass);  null },
        t.getObject  match {case v:Node_URI=>v
                             case _ => null }).filter(_!=null)
  }

*/
  
}