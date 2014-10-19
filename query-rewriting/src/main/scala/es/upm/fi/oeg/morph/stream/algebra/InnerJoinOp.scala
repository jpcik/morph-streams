package es.upm.fi.oeg.morph.stream.algebra
import org.apache.commons.lang.NotImplementedException
import es.upm.fi.oeg.morph.stream.algebra.xpr.BinaryXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.XprUtils

class InnerJoinOp (left:AlgebraOp,right:AlgebraOp)   
  extends JoinOp("join",left,right) {
    
  def simplify={
    if (left==null || right ==null) null
    else this
  }
  
  def join(proj:ProjectionOp):AlgebraOp={
	if (this.left==null) new InnerJoinOp(proj,right)
	else if (this.right==null) new InnerJoinOp(left,proj)
	else new InnerJoinOp(this, proj)				
  }
	
  override def join(newOp:AlgebraOp):AlgebraOp =newOp match{
	  case join:InnerJoinOp=> this.join(join)															 
	  case proj:ProjectionOp=> join(proj)
	  case sel:SelectionOp=>
		if (sel.id.equals(this.left.id)){
		  val inter = this.left.join(newOp)
		  new InnerJoinOp(inter,right)
		  //this.setLeft(inter)
		}
		if (sel.id.equals(this.right.id)){
		  val inter = this.right.join(newOp)
		  new InnerJoinOp(left,inter)
		  //this.setRight(inter)
		}
		else this
			
	  //case union:OpUnion=>return build(union)
	  case union:MultiUnionOp=>
	    new InnerJoinOp(this,union)
			
	  case _=>throw new NotImplementedException("not implemented for "+newOp)
			
	}
	 
}