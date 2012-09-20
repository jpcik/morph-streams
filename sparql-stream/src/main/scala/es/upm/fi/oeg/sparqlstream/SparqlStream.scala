package es.upm.fi.oeg.sparqlstream

object SparqlStream {
  def parse(queryString:String)=
    StreamQueryFactory.create(queryString).asInstanceOf[StreamQuery]
}