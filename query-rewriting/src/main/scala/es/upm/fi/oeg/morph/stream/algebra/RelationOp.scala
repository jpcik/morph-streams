package es.upm.fi.oeg.morph.stream.algebra
import es.upm.fi.oeg.morph.common.TimeUnit

class RelationOp(relationId:String,val extentName:String, val pk:Set[String]) 
  extends UnaryOp(relationId,"relation",null){
  
  override def toString=
	name+" "+extentName
	
  override def copyOp=
	new RelationOp(id,extentName,pk)	
}

class WindowOp(windowId:String,extentName:String,pk:Set[String],val windowSpec:WindowSpec) 
  extends RelationOp(windowId,extentName,pk){
  
  override def copyOp=
	new WindowOp(id, extentName,pk,windowSpec)
	
  override def toString=
	"window"+" "+ extentName+ " "+windowSpec+ " "+pk
	
}

class PatternOp(patternId:String, extentName:String, val pattern:String)
  extends RelationOp(patternId,extentName,null){
  override def copyOp=new PatternOp(id, extentName,pattern)
  override def toString="pattern"+" "+ extentName+ " "+pattern
}


case class WindowSpec(iri:String,from:Long,fromUnit:TimeUnit,to:Long,toUnit:TimeUnit,slide:Long,slideUnit:TimeUnit) {
}