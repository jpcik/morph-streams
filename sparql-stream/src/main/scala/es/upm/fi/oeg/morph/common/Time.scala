package es.upm.fi.oeg.morph.common


class TimeUnit(val factor:Double,val name:String){
  override def toString="TimeUnit("+factor+")"
}

object TimeUnit {
  val MILLISECOND =new TimeUnit(0.001,"millisecond")
  val SECOND= new TimeUnit(1,"second")
  val MINUTE= new TimeUnit(60,"minute")
  val HOUR =new TimeUnit(3600,"hour")
  val DAY =new TimeUnit(3600*24,"day")
  val WEEK =new TimeUnit(3600*24*7,"week")
  val MONTH =new TimeUnit(3600*24*30,"month")
  val YEAR =new TimeUnit(3600*24*365,"year")
  
  def convertToBase(value:Double,unit:TimeUnit)=value*unit.factor

  def convertToUnit(value:Double,unit:TimeUnit,targetUnit:TimeUnit)={
	val inBase = convertToBase(value, unit)
	inBase/targetUnit.factor
  }
}
