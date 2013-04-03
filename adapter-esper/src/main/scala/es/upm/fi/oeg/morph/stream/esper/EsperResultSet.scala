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
import es.upm.fi.oeg.morph.stream.algebra.xpr.OperationXpr
import org.slf4j.LoggerFactory

class EsperResultSet(val records:Stream[Array[Object]], 
    val metadata: Map[String, Xpr],queryVars:Array[String]) extends StreamResultSet {
  private val it = records.iterator
  private var current: Seq[Object] = _
  private val logger = LoggerFactory.getLogger(this.getClass)
  
  override def next:Boolean = {
    if (it.hasNext) {
      current = it.next
      logger.trace(current.mkString("----"))
      true
    } else false
  }

  override def close {}
  override def getHoldability():Int = 0
  override def isClosed():Boolean = false

  protected def createMetadata={
    val md: RowSetMetaData = new RowSetMetaDataImpl
    md.setColumnCount(metadata.size)
    var i=1
    metadata.foreach{e=>
      logger.trace("metadata "+e._1+"--"+e._2)
      md.setColumnLabel(i, e._1)
      md.setColumnType(i, Types.VARCHAR)
      i+=1
    }
    md
  }
  private val metaData: ResultSetMetaData = createMetadata
  
  private val internalLabels=queryVars.zipWithIndex.toMap

  private val compoundLabels={
    val spk=internalLabels.keys.toArray.filter(k=>k.contains('_')).map{k=>
      val sp=k.split('_')
      (sp(0),sp(1))      
    }
    val grouped=spk.groupBy(_._1).map(v=>(v._1,v._2.map(v2=>v2._2)))
    grouped
  }
  
  private val labelPos=
    (1 to metaData.getColumnCount).map(i=>metaData.getColumnLabel(i)->i).toMap
    
  override def findColumn(columnLabel:String):Int = labelPos(columnLabel)
  override def getMetaData:ResultSetMetaData=metaData
  override def getObject(columnIndex:Int):Object=current(columnIndex-1)
  
  override def getObject(columnLabel:String):Object={ 
    logger.trace(internalLabels.mkString)

    metadata(columnLabel) match{
      case rep:ReplaceXpr=>
        val repl=internalLabels.map(l=>l._1.replace(columnLabel+"_","")->current(l._2))
        rep.evaluate(repl)
      case v:VarXpr=>
        current(internalLabels(columnLabel))
      case NullValueXpr=>null
      case op:OperationXpr=>op.evaluate
    }
  }

  override def getString(columnLabel:String):String = 
    getObject(columnLabel:String).toString
}

class EsperCompResultSet(rss:Seq[EsperResultSet]) extends StreamResultSet{
  private val it=rss.iterator
  private var current:EsperResultSet=null
  override def next:Boolean = {
    if (current==null) current=it.next
    val nxt=current.next
    if (nxt) nxt
    else if (it.hasNext){
      current=it.next
      current.next
    }
    else false       
  }
  
  override def findColumn(columnLabel:String):Int =rss.head.findColumn(columnLabel)    
  override def getMetaData:ResultSetMetaData=rss.head.getMetaData
  override def getObject(columnIndex:Int):Object=current.getObject(columnIndex)
  override def getObject(columnLabel:String):Object=current.getObject(columnLabel)  
  override def getString(columnLabel:String):String = current.getString(columnLabel)
}