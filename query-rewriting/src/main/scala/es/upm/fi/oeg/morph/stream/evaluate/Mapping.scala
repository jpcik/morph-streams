package es.upm.fi.oeg.morph.stream.evaluate

import java.net.URI

case class Mapping(uri:Option[URI],data:Option[String]=None){
  //def this(uri:URI)=this(Some(uri))
  //def this(mappingData:String)=this(None,Some(mappingData))
}
object Mapping{
  def apply(uri:URI)=new Mapping(Some(uri))
  def apply(mappingData:String)=new Mapping(None,Some(mappingData))
}
