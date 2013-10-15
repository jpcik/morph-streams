package es.upm.fi.oeg.morph.stream.rewriting

import es.upm.fi.oeg.sparqlstream.StreamQuery
import com.hp.hpl.jena.sparql.syntax.Element
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock
import com.hp.hpl.jena.sparql.syntax.ElementGroup
import collection.JavaConversions._
import com.hp.hpl.jena.sparql.syntax.ElementUnion
import com.hp.hpl.jena.sparql.core.TriplePath
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.sparqlstream.syntax.ElementNamedStream

object QueryReordering {
  val logger=LoggerFactory.getLogger(getClass)
  def reorder(query:StreamQuery):StreamQuery={       
    query.setQueryPattern(reorder(query.getQueryPattern))
    logger.debug("reordered "+query)
    query
  }
  
  def groupTriples(root:TriplePath,triples:Seq[TriplePath]):Element={
    val g=new ElementGroup
    g.addTriplePattern(root.asTriple)
    val children=triples.filter(t=> t.getSubject==root.getObject)
    val rest=triples.filter(t=> t.getSubject!=root.getObject)
    children.foreach{t=>
      //g.addTriplePattern(t.asTriple)
      g.addElement(groupTriples(t,rest))
    }
    if (g.getElements.size==1)
      g.getElements.head
    else g        
  }
  
  def reorderTriples(triples:Seq[TriplePath]):Element={
    val objects=triples.map(t=>t.getObject)
    val roots=triples.filter(t => !objects.contains(t.getSubject))
    

    val group=new ElementGroup
    roots.sortBy(_.getSubject.toString)
    .foreach{root=>
      val g=groupTriples(root,triples.filterNot(t=>t.equals(root) || roots.contains(t)))
      group.addElement(g)
    }
    group
  }
  
  def reorder(element:Element):Element= element match{
     case group:ElementGroup=> val g=new ElementGroup() 
       group.getElements.foreach(e=>g.addElement(reorder(e)))
       g
     case triples:ElementPathBlock=>       
        val trips=triples.getPattern.getList
        reorderTriples(trips)
     case union:ElementUnion=>
       val els=union.getElements.map(e=>reorder(e))
       union.getElements.clear()
       union.getElements.addAll(els)
       union
     case sgraph:ElementNamedStream=>
       new ElementNamedStream(sgraph.node,reorder(sgraph.element),sgraph.window)
     case _ =>element
  }
}