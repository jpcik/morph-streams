package es.upm.fi.oeg.morph.stream.algebra.xpr

class AggXpr(val aggOp:AggFunction,val varName:String) 
  extends OperationXpr(aggOp.opName,new VarXpr(varName)){

  override def toString()=aggOp.opName+"("+varName+")"
}

class AggFunction(val opName:String)
object MaxXpr extends AggFunction("max")
object MinXpr extends AggFunction("min")
object SumXpr extends AggFunction("sum")
object AvgXpr extends AggFunction("avg")
object CountXpr extends AggFunction("count")
