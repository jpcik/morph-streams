package es.upm.fi.oeg.siq.wrapper

import es.emt.wsdl.ServiceGEOSoap12Bindings
import scala.xml._


object Emt {
  
class EmtServ extends ServiceGEOSoap12Bindings with scalaxb.SoapClients with scalaxb.DispatchHttpClients{
  
  
}

    //).getQuote(Some("GOOG")))

def main(args:Array[String]){
  val w= new PollWrapper
  w.start()
  Thread.sleep(10000)
  w.stop()
  /*
  val service = new EmtServ().service//(new  ServiceGEOSoap12Bindings with scalaxb.SoapClients with scalaxb.DispatchHttpClients {}).service
  val data=(service.getArriveStop(Some("WEB.SERVICIOS.FIUPM"),
          Some("05D413C8-BCEB-425E-81CB-B1C2DB96CC70"),Some("3"),Some(""),Some("")))
  val x = <baba/>
  val res=data.right.get.getArriveStopResult.get.mixed.head.value.asInstanceOf[Elem]
  val dats=(res \ "Arrive").map{arr=>    
    Array( (arr \ "idLine").head.text,
    (arr \ "TimeLeftBus").head.text,
    (arr \ "DistanceBus").head.text)
  }
  dats.foreach{rec=>
   println(rec.mkString(";;"))
  }*/

}

}

