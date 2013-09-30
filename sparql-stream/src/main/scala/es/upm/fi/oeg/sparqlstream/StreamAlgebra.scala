package es.upm.fi.oeg.sparqlstream

object StreamAlgebra {
   def compile(query:StreamQuery)={
     if (query == null) null 
     else new StreamAlgebraGenerator().compile(query) 
   }
}