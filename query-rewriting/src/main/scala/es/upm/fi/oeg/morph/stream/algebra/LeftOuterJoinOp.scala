package es.upm.fi.oeg.morph.stream.algebra
import collection.JavaConversions._

class LeftOuterJoinOp(left:AlgebraOp,right:AlgebraOp)  
  extends JoinOp("leftjoin",left,right) {	
}