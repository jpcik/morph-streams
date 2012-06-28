package es.upm.fi.oeg.morph.stream.gsn
import es.upm.fi.oeg.morph.stream.query.SqlQuery
import es.upm.fi.oeg.morph.stream.algebra.InnerJoinOp
import es.upm.fi.oeg.morph.stream.algebra.LeftOuterJoinOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp
import org.apache.commons.lang.NotImplementedException
import es.upm.fi.dia.oeg.integration.Template
import com.google.common.collect.Maps
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.OperationXpr
import es.upm.fi.oeg.morph.stream.algebra.RootOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.algebra.WindowOp
import es.upm.fi.oeg.morph.stream.algebra.RelationOp
import es.upm.fi.oeg.morph.stream.algebra.SelectionOp
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr

class GsnQuery(projectionVars:Map[String,String]) extends SqlQuery(projectionVars){   
  
  var vars:Map[String,Seq[String]]=_
  var expressions:Map[String,Xpr]=_
  
  override def load(op:AlgebraOp){
	super.load(op)
	this.innerQuery = build(op)
	loadModifiers(op)
	vars=varMappings(op)
	expressions=varXprs(op)
  }

  override def getProjection:Map[String,String]={
    projectionVars.map(p=>p._1->null)
  }
  
  override def supportsPostProc=false
  
  private def varMappings(op:AlgebraOp):Map[String,Seq[String]]=op match{
    case root:RootOp=>varMappings(root.subOp)
    case proj:ProjectionOp=>proj.getVarMappings.filter(_._2!=null)
    case _=>Map[String,Seq[String]]()
  }
  
  private def varXprs(op:AlgebraOp):Map[String,Xpr]=op match{
    case root:RootOp=>varXprs(root.subOp)
    case proj:ProjectionOp=>proj.expressions
    case _=>Map[String,Xpr]()
  }
  
  val gconstants = Maps.newHashMap[String,String]
  val gstaticConstants = Maps.newHashMap[String, Template]();
  val gtemplates =  Maps.newHashMap[String, Template]

	private def loadModifiers(op:AlgebraOp)
	{
		if (op == null)
		{	}
		else op match{
		  case root:RootOp=>
			loadModifiers(root.subOp)
		
		  case proj:ProjectionOp=>
			val extent = proj.getRelation.extentName.toLowerCase();
			//int pos =0;
			
			proj.expressions.entrySet.foreach{entry=>
	
				//select += entry.getValue().toString()+ " AS "+entry.getKey();
				val lowerkey = entry.getKey().toLowerCase();
				if (entry.getValue().isInstanceOf[OperationXpr])
				{
					val opXpr = entry.getValue().asInstanceOf[OperationXpr]
					if (opXpr.op.equals("postproc"))
						gconstants.put(entry.getKey().toLowerCase(), opXpr.param.toString());
					else if (opXpr.op.equals("constant"))
					{
						if (!gstaticConstants.containsKey(extent))
						{
							val template= new Template(extent);
							template.addModifier(lowerkey, opXpr.param.toString());
							gstaticConstants.put(extent, template);
						}
						else
							gstaticConstants.get(extent).addModifier(lowerkey, opXpr.param.toString());
						//staticConstants .put(lowerkey+extent, opXpr.getParam().toString());
					}
				}
				if (entry.getValue().isInstanceOf[VarXpr])
				{
					val vari = entry.getValue().asInstanceOf[VarXpr] 
					/*
					if (vari.getModifier()!=null)
					{	
						if (!gtemplates.containsKey(extent))
						{
							val template = new Template(extent);
							template.addModifier(lowerkey, vari.getModifier());
							gtemplates.put(extent,template);
						}
						else
							gtemplates.get(extent).addModifier(lowerkey, vari.getModifier());
					}*/
				}
			}
			loadModifiers(proj.subOp)
			//yield(proj);
			//return "(SELECT "+ select+" FROM "+build(proj.getSubOp())+")";
		
		case op:WindowOp=>{
			//OpWindow win = (OpWindow)op;
			//return win.getExtentName()+ serializeWindowSpec(win.getWindowSpec());
		}
		case op:RelationOp=>
		{
			//OpRelation rel = (OpRelation)op;
			//return rel.getExtentName();
		}
		//else if (op.getName().equals("join"))
		case union:MultiUnionOp=>
			union.children.values.foreach{child=>
				loadModifiers(child);
			}
		
		case join:InnerJoinOp=>
			loadModifiers(join.left)
			loadModifiers(join.right)
		case _=>
		{
			
			//return "";
		}
		
		}
	}

  
  
  

  private def build(op:AlgebraOp):String={
	if (op == null)	return "";
	else op match{
	  case root:RootOp=>return build(root.subOp)
	  //case union:OpUnion=>return build(union.getLeft)+" UNION  "+build(union.getRight)			
      case proj:ProjectionOp=>
        "field[0]="+projVars(proj).map(trimExtent(_)).filterNot(_.equals("timed")).mkString(",")+"&"+build(proj.subOp)
	  case win:WindowOp=> throw new NotImplementedException("NYI Window")
	    //return win.getExtentName+ serializeWindowSpec(win.getWindowSpec)+ " "+win.getExtentName
      case rel:RelationOp=> "vs[0]="+rel.extentName
	  case sel:SelectionOp=>
		return build(sel.subOp)+ " WHERE "+serializeExpressions(sel.expressions,null)
	  case join:LeftOuterJoinOp=>throw new NotImplementedException("NYI Left Join")
	  /*
	    var select = "SELECT "+projVars(join)+" FROM "+ get(join).mkString(",") 
		if (!join.conditions.isEmpty)
			select+=" WHERE "+joinXprs(join).mkString(" AND ")+" "+conditions(join).mkString(" AND ")
	    select*/
	  case join:InnerJoinOp=>throw new NotImplementedException("NYI Join")
	  /*
		var select = "SELECT "+projVars(join)+//serializeSelect(opLeft,"",true)+ ", "+serializeSelect(opRight,"2",true) +
				" FROM "+ get(join).mkString(",") //build(opLeft.getSubOp())+","+build(opRight.getSubOp());
		if (!join.conditions.isEmpty())
			select+=" WHERE "+joinXprs(join).mkString(" AND ")+" "+conditions(join).mkString(" AND ");
			
	  return "("+select+")";*/
		
	  case union:MultiUnionOp=>
		return union.children.values.map(col=>build(col)).mkString(" UNION ")
	  case _=>throw new Exception("Unsupported operator: "+op)
	}
  }

}
