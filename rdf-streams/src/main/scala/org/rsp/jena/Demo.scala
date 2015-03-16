package org.rsp.jena

import com.hp.hpl.jena.rdf.model._
import com.hp.hpl.jena.vocabulary.VCARD
import scala.collection.JavaConversions._
import org.apache.jena.riot.RDFDataMgr
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.vocabulary.{DC_11=>DC}
import org.openjena.atlas.io.IndentedWriter
import com.hp.hpl.jena.query._
import com.hp.hpl.jena.query.ResultSetFactory._
import com.hp.hpl.jena.sparql.vocabulary.FOAF
import scala.concurrent.Future
import org.apache.jena.riot.system.StreamRDFLib
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory
import org.apache.jena.riot.system.StreamRDF
import com.hp.hpl.jena.graph.Triple
import org.apache.jena.riot.system.StreamRDFLib
import org.apache.jena.riot.system.StreamOps
import com.hp.hpl.jena.sparql.core.Quad
import akka.actor._
import org.semanticweb.owlapi.apibinding.OWLManager
import eu.trowl.owlapi3.rel.tms.reasoner.dl.RELReasonerFactory

object Demo {
import JenaPlus._
import ModelFactory._
  def test1={
    val personURI = "http://somewhere/JohnSmith"
    implicit val model = createDefaultModel
    add(personURI,VCARD.FN->"John Smith")    
    
    model.write(System.out,"TTL")
  }

  def test2={
    val (personURI,givenName,familyName) = ("http://somewhere/JohnSmith","John","Smith")
    val fullName = s"$givenName $familyName"
    implicit val model = createDefaultModel
    add(personURI,VCARD.FN->fullName,
                  VCARD.N ->add(bnode,VCARD.Given-> givenName,
                                      VCARD.Family->familyName))
    model.write(System.out,"TTL")

  }
  def test3={
    val personURI = "http://somewhere/JohnSmith"
    val givenName = "John"
    val familyName = "Smith"
    val fullName = s"$givenName $familyName"
    implicit val model = createDefaultModel
      
    iri(personURI)~(VCARD.FN,fullName)~
                   (VCARD.N,"Smitho")~
                   (VCARD.N, bnode~(VCARD.Given, givenName)~
                                   (VCARD.Family,familyName))
                                                          
    model.listStatements.foreach{stmt=>      
      print(s"${stmt.sub} ${stmt.pred} " )
      print(stmt.obj match {
        case r:Resource=>r.toString
        case l:Literal => "\""+l.toString+"\"" 
      })
      println(".")
    }
    
    val names=model.listObjectsOfProperty(VCARD.N).map{
      case r:Resource=> r.getProperty(VCARD.Family).obj.toString
      case l:Literal=>  l.getString
    }
    
    println(names.mkString(","))
  }
  
  def test4={
    val m=createDefaultModel
    RDFDataMgr.read(m, "690_2005_8_29.n3")
    m.listStatements.foreach{s=>println(s)}
    
    val preds=m.listStatements.map{s=>s.pred.getLocalName}.toSet
    preds.foreach(println)
    
    m.listObjects.foreach{
      case l:Literal=> println(l.getDouble)
      case r:Resource => println(r.getLocalName)
    }
    
  }
  
  def test5={
    val nl=System.lineSeparator
    implicit val m = createDefaultModel
    iri("http://example.org/book#1")
             .prop(DC.title, "SPARQL - the book")
             .prop(DC.description, "A book about SPARQL") 
    iri("http://example.org/book#2") 
             .prop(DC.title, "Advanced techniques for SPARQL")
             
    val queryString = s"""PREFIX dc: <${DC.getURI}> $nl
                          SELECT ?title 
                          WHERE {?x dc:title ?title}"""
    val query = sparql(queryString) 
    val result = query.select(m)
    //ResultSetFormatter.out(result)
    
    result foreach{implicit sol=>      
      println("got this: "+lit("title"))
    }
    
    query.serialize(new IndentedWriter(System.out,true)) 
    println
    
    /*
    val rewindable = makeRewindable(query.select(m)) 
    ResultSetFormatter.out(rewindable) 
    rewindable.reset
    ResultSetFormatter.out(rewindable)*/ 
           
  }
  
  def test6={
    val queryStr = """select distinct ?Concept 
                      where {[] a ?Concept} LIMIT 10"""
    val query = sparql(queryStr)

    val resos:ResultSet = query.serviceSelect("http://dbpedia.org/sparql") 
    resos.foreach{implicit qs=>
      println(res("Concept").getURI)
    }
// Set the DBpedia specific timeout.
//((QueryEngineHTTP)qexec).addParam("timeout", "10000") ;
// Execute.
//ResultSetFormatter.out(resos);
    
  }
  
  def test7={
    import concurrent.ExecutionContext.Implicits.global 
    val rdfs=("rdfs","http://www.w3.org/2000/01/rdf-schema#")
    val foaf=("foaf","http://xmlns.com/foaf/0.1/")
    val dct=("dct","http://purl.org/dc/terms/")
    val dbo=("dbo","http://dbpedia.org/ontology/")
    val default=("","http://dbpedia.org/resource/")
      def pref(s:(String,String))=s"PREFIX ${s._1}: <${s._2}>"
      def prefs(p:collection.Seq[(String,String)])=
        p.map(pref(_)).mkString("\n")
      def prefns(p:(String,String)*)=
        p.map(pref(_)).mkString("\n")
     val queryStr = s"""
          ${prefns(rdfs,foaf,dct,dbo,default)}

          SELECT (min(?gname) as ?givname) ?name ?description ?person 
          WHERE {
             ?person dbo:birthPlace :Hamburg.
		     ?person dct:subject :Category:German_musicians .
		     ?person dbo:birthDate ?birth .
		     ?person foaf:surname ?name .
		     ?person foaf:givenName ?gname .
		     ?person rdfs:comment ?description .
		     FILTER (LANG(?description) = 'en') .
          } 
          group BY ?name ?description ?person"""
     // println(queryStr)
    val query = sparql(queryStr)
    val f=Future(query.serviceSelect("http://dbpedia.org/sparqll")).fallbackTo(
          Future(query.serviceSelect("http://dbpedia.org/sparql")))
    f.recover{ case e=> println("Error "+e.getMessage)}
    
    f.map(_.foreach{implicit qs=>
        println(lit("givname").getValue)
    })
    
    println("after all")
    //ResultSetFormatter.out(resos);

  }

  def test8={
    implicit val m=createDefaultModel
//@prefix : <http://example.org/> .
//@prefix foaf: <http://xmlns.com/foaf/0.1/> .
    val ex="http://example.org/"
    val alice=iri(ex+"alice")
    val bob=iri(ex+"bob")
    val charlie=iri(ex+"charlie")
      
    alice+(RDF.`type`->FOAF.Person,
            FOAF.name->"Alice",
            FOAF.mbox->iri("mailto:alice@example.org"),
            FOAF.knows->bob,
            FOAF.knows->charlie,
            FOAF.knows->bnode)
    bob+    (FOAF.name->"Bob",
            FOAF.knows->charlie)
    charlie+(FOAF.name->"Charlie",
            FOAF.knows->alice)
            
    m.listStatements.foreach {stmt=>
      stmt sub match{
        case Iri(iri)=>print("URI ")
        case Bnode(id)=>print("Blank ")}
      stmt pred match{
        case Iri(iri)=>print("URI ")}

      stmt obj match{
        case Iri(iri)=>print("URI")
        case Bnode(id)=>print("Blank")
        case _ => print("Literal")}
		println
    }      
    alice.listProperties(FOAF.knows).foreach{s=>s obj match {
      case Bnode(id)=>println(id)
      case Iri(iri) => println(iri)        
    }}
    
  }  
  import StreamOps._
  def test9={
    implicit val m=createDefaultModel
    RDFDataMgr.read(m, "690_2005_8_29.n3")
    
    val ds=DatasetFactory.create(m).asDatasetGraph    
    sendDatasetToStream(ds, new Streamer)
    
  }

  val sys=ActorSystem.create("system")
  val consumer=sys.actorOf(Props[RdfConsumer])
  
  class Streamer extends StreamRDF{
    override def triple(triple:Triple){
      consumer ! triple
    }
    
    def quad(q:Quad)={}
    def base(base:String ) ={}
    def prefix(pref:String,iri:String) ={}
    def finish() ={}
    def start() ={}
  }
  
  class RdfConsumer extends Actor{
     def receive= {
       case t:Triple => 
         if (t.predicateMatches(RDF.`type`))
           println(s"received triple $t")
      }
  }
  
  def test10={
    import org.rsp.owlapi.OwlApiTips._
    val pref="http://example.org/onto#"
    implicit val mgr=OWLManager.createOWLOntologyManager
    implicit val fac=mgr.getOWLDataFactory
    val onto=mgr.createOntology

    val artist=clazz(pref+"Artist")
    val singer=clazz(pref +"Singer")    
    onto += singer subClassOf artist

    val reasoner = new RELReasonerFactory().createReasoner(onto)

    val elvis=ind(pref+"Elvis")
    reasoner += elvis ofClass singer

    reasoner.reclassify
    
    reasoner.getIndividuals(artist) foreach{a=>
      println(a.getRepresentativeElement.getIRI)
    }
    

  }
  
  def main(args:Array[String]):Unit={
    test10
    //Thread.sleep(5000)
  }
  
}