package es.upm.fi.oeg.morph.kyrie

import es.upm.fi.dia.oeg.newrqr.DatalogSPARQLConversor
import org.oxford.comlab.requiem.parser.ELHIOParser
import org.oxford.comlab.requiem.rewriter.TermFactory
import org.oxford.comlab.requiem.rewriter.PreprocessRewriter
import java.util.ArrayList
import com.hp.hpl.jena.query.Query
import org.oxford.comlab.requiem.rewriter.Clause
import es.upm.fi.dia.oeg.newrqr.ISI2RQRLexer
import es.upm.fi.dia.oeg.newrqr.ISI2RQRParser
import org.antlr.runtime.ANTLRStringStream
import org.antlr.runtime.CommonTokenStream
import collection.JavaConversions._

class Kyrie (ontologyFile:String){
  private val dsc = new DatalogSPARQLConversor
  private val m_parser = new ELHIOParser(new TermFactory, true)
  val ontoClauses=m_parser.getClauses(ontologyFile);
  private val pw = new PreprocessRewriter(new ArrayList[Clause](ontoClauses), "F")

  def clausify(q:Query)=dsc.sparqlToDatalog(q).toSeq  
  
  def rewriteDatalogString(datalogQuery:String)= {
    val lexer = new ISI2RQRLexer(new ANTLRStringStream(datalogQuery))
    val parser = new ISI2RQRParser(new CommonTokenStream(lexer))
    rewriteDatalogClauses(parser.program)
  }
  
  def rewriteDatalogClauses(clauses:Seq[Clause])=
    pw.rewrite(new ArrayList(clauses)).toSeq
}