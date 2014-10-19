package org.rsp

import akka.actor.Actor
import com.hp.hpl.jena.graph.Triple

class RspTrowl extends Actor{
  def receive={
    case t:Triple=>
  }
}