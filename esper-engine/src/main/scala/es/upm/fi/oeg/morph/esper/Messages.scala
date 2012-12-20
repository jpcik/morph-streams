package es.upm.fi.oeg.morph.esper

case class Event(name:String,attributes:Map[String,Any])
case class ExecQuery(query:String)
case class RegisterQuery(query:String)
case class PullData(id:String)
case class Ping(msg:String)
case class CreateWindow(name:String,window:String,duration:String)
