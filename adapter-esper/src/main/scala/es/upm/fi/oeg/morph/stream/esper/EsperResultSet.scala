package es.upm.fi.oeg.morph.stream.esper
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.evaluate.StreamResultSet
import javax.sql.rowset.RowSetMetaDataImpl
import javax.sql.RowSetMetaData
import java.sql.Types
import java.sql.ResultSetMetaData
import es.upm.fi.oeg.morph.stream.algebra.xpr.ReplaceXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.NullValueXpr

class EsperResultSet(val records: collection.immutable.Stream[Array[Object]], 
    val metadata: Map[String, Xpr],queryVars:Array[String]) extends StreamResultSet {
  val it = records.iterator
  var current: Seq[Object] = _

  override def next:Boolean = {
    if (it.hasNext) {
      current = it.next
      println(current.mkString("-------------"))
      true
    } else false
  }

  override def close {
    //records
  }

  override def getHoldability(): Int = 0
  override def isClosed(): Boolean = false

  protected def createMetadata={
    val md: RowSetMetaData = new RowSetMetaDataImpl
    md.setColumnCount(metadata.size)
    var i=1
    metadata.foreach{e=>
      println("metadata "+e._1+"--"+e._2)
      md.setColumnLabel(i, e._1)
      md.setColumnType(i, Types.VARCHAR)
      i+=1
    }
    //md.setColumnLabel(i,"extentname")
    md
  }
  private val metaData: ResultSetMetaData = createMetadata
  
  private val internalLabels={
    val vars=queryVars.zipWithIndex.toMap
    //val vars=metadata.map(m=>m._2.varNames).flatten.filterNot(_.equals("timed"))++List("timed")
    //println(vars.mkString("::::::::"))
    vars//.zipWithIndex.toMap
  }
  private val labelPos=
    (1 to metaData.getColumnCount).map(i=>metaData.getColumnLabel(i)->i).toMap
    
  override def findColumn(columnLabel: String): Int = 
    labelPos(columnLabel)
    
  override def getMetaData:ResultSetMetaData=metaData

  override def getObject(columnIndex:Int):Object={    
    current(columnIndex-1)
  }

  override def getObject(columnLabel:String):Object={ 
    metadata(columnLabel) match{
      case rep:ReplaceXpr=>
        //val replaceVals=rep.vars.map(v=>v.varName->current(internalLabels(v.varName))).toMap
        println(internalLabels)
        rep.evaluate(internalLabels.map(l=>l._1->current(l._2)).toMap)
      case v:VarXpr=>
        println("we get this: "+v)
        current(internalLabels(v.varName))
      case NullValueXpr=>null
    }
  }

  override def getString(columnLabel: String): String = getObject(columnLabel: String).toString
}

