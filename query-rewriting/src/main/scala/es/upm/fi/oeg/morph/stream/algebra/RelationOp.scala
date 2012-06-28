package es.upm.fi.oeg.morph.stream.algebra

import es.upm.fi.dia.oeg.integration.algebra.Window

class RelationOp(relationId:String,val extentName:String) 
  extends UnaryOp(relationId,"relation",null){
  
  override def toString=
	name+" "+extentName
	
  override def copyOp=
	new RelationOp(id,extentName)	
}

class WindowOp(windowId:String,extentName:String,val windowSpec:Window) 
  extends RelationOp(windowId,extentName){
  
  override def copyOp=
	new WindowOp(id, extentName,windowSpec)
	
  override def toString=
	"window"+" "+ extentName+ " "+windowSpec
	
}