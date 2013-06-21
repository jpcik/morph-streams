package es.upm.fi.oeg.morph.stream.rewriting

import com.hp.hpl.jena.sparql.syntax.Element
import org.oxford.comlab.requiem.rewriter.Term
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import collection.JavaConversions._
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock
import org.slf4j.LoggerFactory
import com.hp.hpl.jena.sparql.core.TriplePath
import es.upm.fi.oeg.morph.voc.RDF
import org.oxford.comlab.requiem.rewriter.FunctionalTerm
import com.hp.hpl.jena.graph.Node
import org.oxford.comlab.requiem.rewriter.TermFactory
import es.upm.fi.oeg.sparqlstream.StreamQuery
import org.oxford.comlab.requiem.rewriter.Clause
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.syntax.ElementUnion
import org.oxford.comlab.requiem.rewriter.Variable
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.graph.Node_URI

class SparqlClausifier(tf:TermFactory, query:Query) {
  private val logger=LoggerFactory.getLogger(this.getClass)
  val mappedVars=vars(query.getQueryPattern).toSet.zipWithIndex.toMap
  val projVars=query.getProjectVars.map(v=>v.getVarName).toSeq.filter(v=>mappedVars.contains(v))
  val inverseVars=mappedVars.map(m=>"?"+m._2.toString->m._1)
  
  lazy val clausifiedQuery={
    val qclauses=clausify(query.getQueryPattern,mappedVars).toArray
    val head=new FunctionalTerm("Q",projVars.map(v=>tf.getVariable(mappedVars(v))).toArray)
    new Clause(qclauses,head)
  }
  
  def sparqlizeUCQ(fclauses:Seq[Clause])={
    
    val clauses=fclauses.map(c=>replaceVars(c,clausifiedQuery))
    
    val pb=new ElementPathBlock()
    val blocks=clauses.map{c=>
      val pb=new ElementPathBlock      
      c.getBody.foreach{t=>
        val triple=triplify(t,null,inverseVars)
        pb.addTriple(triple)
      }
      pb
    }
    val union = new ElementUnion
    blocks.foreach(b=>union.addElement(b))
    val rep=replaceElems(query.getQueryPattern, union)
    query.setQueryPattern(rep)
   query
  }

  private def replaceVars(c:Clause,masterClause:Clause)={
    val head=masterClause.getHead
    val vars=head.getArguments.map(v=>v.getName.replace("?","").toInt)
    val termVars=c.getHead.getArguments.map(v=>v.getName.replace("?","").toInt)    
    val repl=termVars.zip(vars).filter(pair=>pair._1!=pair._2).toMap
    val vars1=masterClause.getBody.map{t=>
      t.getName->t.getArguments.filter(a=>a.isInstanceOf[Variable])
        .map(v=>v.getName.replace("?","").toInt)      
    }.toMap
    val replc=c.getBody.map{t=>
      val tt=vars1.getOrElse(t.getName, Array())
      val tt2=t.getArguments.filter(a=>a.isInstanceOf[Variable])
      .map(v=>v.getName.replace("?","").toInt)
      tt2.zip(tt)
    }.flatten.filter(pair=>pair._1!=pair._2).toMap
      
    
    
    logger.debug("replacing vars: "+replc.mkString(","))
    new Clause(c.getBody.map(t=>replaceVar(replc,t)),head)
  }
  
  private def replaceVar(vars:Map[Int,Int],t:Term):Term={
    val args=t.getArguments.map{arg=> arg match{
      case v:Variable=>
        val vv=v.getName.replace("?","").toInt
        logger.debug("Now replacing var: "+ vv)
        if (vars.contains(vv)) tf.getVariable(vars(vv))
        else v
      case fTerm:FunctionalTerm=>replaceVar(vars,fTerm)
      case _=>arg
    }}
    new FunctionalTerm(t.getName,args)
  }

  
  
  //TODO: generalize for more union replacements
  private def replaceElems(e:Element,union:ElementUnion):Element=e match{
    case group:ElementGroup=>
      val ng=new ElementGroup()      
      ng.getElements.addAll(group.getElements.map(el=>replaceElems(el,union)).toList)
      ng
    case pathblock:ElementPathBlock=>union
    case _=>e
  }
  
  
  private def triplify(t:Term,vocab:Map[String,Node_URI],vars:Map[String,String])={
    //TODO keep original namespaces
    val pref=""//"http://purl.oclc.org/NET/ssnx/ssn#"
    val predName=if (t.getArity==1) RDF.typeProp 
      else ResourceFactory.createResource(pref+t.getName)
    val subj=t.getArgument(0).getName
    val obj= if (t.getArity==1) ResourceFactory.createResource(pref+t.getName).asNode
      else if (t.getArgument(1).getName.startsWith("?")) 
        Var.alloc(vars(t.getArgument(1).getName))
      else ResourceFactory.createResource(pref+t.getArgument(1).getName).asNode
    logger.debug("trubila "+subj+ " "+predName+" "+obj)
    
    new com.hp.hpl.jena.graph.Triple( Var.alloc(vars(subj)),predName.asNode, obj)        
  }

  
  private def clausify(el:Element,vars:Map[String,Int]):Seq[Term]=el match{
      case group:ElementGroup=>group.getElements.map{e=>clausify(e,vars)}.flatten
      case triples:ElementPathBlock=>
        logger.debug(triples.toString)
        sclausify(triples.getPattern.getList,vars)
      case _ =>Seq()
  }
  
  private def sclausify(triples:Seq[TriplePath],vars:Map[String,Int])={
    triples.map{t=>
      if (t.getPredicate.getURI.equals(RDF.typeProp.getURI))
        new FunctionalTerm(t.getObject.getURI,
            Array(createTerm(t.getSubject,vars)))  
      else new FunctionalTerm(t.getPredicate.getURI,
            Array(createTerm(t.getSubject,vars),createTerm(t.getObject,vars)))
    }.toArray       
  }
  
  private def createTerm(node:Node,vars:Map[String,Int])={
    if (node.isVariable) tf.getVariable(vars(node.getName))
    else if (node.isLiteral) tf.getConstant(node.getLiteralValue.toString)
    else tf.getConstant(node.getURI)    
  }
 
    
  private def vars(el:Element):Seq[String]=el match{
    case group:ElementGroup=>group.getElements.map(vars(_)).flatten
    case path:ElementPathBlock=>path.getPattern.map(t=>vars(t)).toSeq.flatten
    case _ => Seq()
  }
  
  private def vars(t:TriplePath)={
    Seq(t.getSubject match {case v:Var=>v.getVarName
                             case _ =>null  },
        t.getObject  match {case v:Var=>v.getVarName
                             case _ => null }).filter(_!=null)
  }
  
   
}