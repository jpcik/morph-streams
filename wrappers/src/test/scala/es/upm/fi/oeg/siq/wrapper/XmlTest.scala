package es.upm.fi.oeg.siq.wrapper

import scala.xml.Elem

object XmlTest {

  val servicefields=Seq("idLine","TimeLeftBus","DistanceBus")
  val xml= <Arrives>
             <Arrive>
               <IdStop>20</IdStop><idLine>162</idLine><IsHead>True</IsHead>
               <Destination>MONCLOA</Destination><IdBus>0000</IdBus>
               <TimeLeftBus>654</TimeLeftBus><DistanceBus>1494</DistanceBus>
               <PositionXBus>-1</PositionXBus><PositionYBus>-1</PositionYBus>
               <PositionTypeBus>1</PositionTypeBus>
             </Arrive><Arrive>
               <IdStop>20</IdStop><idLine>162</idLine><IsHead>True</IsHead>
               <Destination>MONCLOA</Destination><IdBus>0000</IdBus>
               <TimeLeftBus>999999</TimeLeftBus><DistanceBus>8797</DistanceBus>
               <PositionXBus>-1</PositionXBus><PositionYBus>-1</PositionYBus>
               <PositionTypeBus>1</PositionTypeBus>
             </Arrive></Arrives>
  

   def extract(xml:Elem) ={
    val c=xml.child
    println(c.size)
     xml.child.filter(_.isInstanceOf[Elem]).map{e=>
       println("chip"+e.getClass())
      var i=0
      servicefields.map{key=>
        val str= (e \ key).head
        i+=1
        println(str.text.toInt)         
       }
     } 
   }

  def main(args:Array[String]):Unit={
    extract(xml)
  }
}