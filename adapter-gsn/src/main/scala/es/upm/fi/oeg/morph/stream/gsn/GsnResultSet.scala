package es.upm.fi.oeg.morph.stream.gsn
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import javax.sql.RowSetMetaData
import javax.sql.rowset.RowSetMetaDataImpl
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.io.Reader
import java.sql.RowId
import java.io.InputStream
import java.sql.Blob
import java.sql.Ref
import java.sql.Clob
import java.util.Calendar
import java.sql.NClob
import java.sql.SQLXML
import java.sql.Statement
import java.net.URL
import java.sql.SQLWarning
import java.sql.Types
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ReplaceXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.stream.evaluate.StreamResultSet

class GsnResultSet(val records: Stream[Array[String]], val metadata: Map[String, Xpr]) 
  extends StreamResultSet {
  val it = records.iterator
  var current: Seq[String] = _
  val logger=LoggerFactory.getLogger(this.getClass)

  override def next:Boolean = {
    if (it.hasNext) {
      current = it.next
      println(current.mkString("-"))
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
      logger.debug("metadata "+e._1+".")
      md.setColumnLabel(i, e._1)
      md.setColumnType(i, Types.VARCHAR)
      i+=1
    }
    //md.setColumnLabel(i,"extentname")
    md
  }
  private val metaData: ResultSetMetaData = createMetadata
  
  private val internalLabels={
    val vars=metadata.map(m=>m._2.varNames).flatten.toSet.filterNot(_.equals("timed"))++List("timed")
    logger.debug(vars.mkString("::::::::"))
    vars.zipWithIndex.toMap
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
    logger.trace("get object by label: %s." format columnLabel)
    metadata(columnLabel) match{
      case rep:ReplaceXpr=>
        //val replaceVals=rep.vars.map(v=>v.varName->current(internalLabels(v.varName))).toMap
        rep.evaluate(internalLabels.map(l=>l._1->current(l._2)).toMap)
      case v:VarXpr=>current(internalLabels(v.varName))
    }
  }

  override def getString(columnLabel: String): String = getObject(columnLabel: String).toString
  
}


class GsnMultiResultSet(resultSets:Array[GsnResultSet]) extends GsnResultSet(Stream(),Map()){
  val iter=resultSets.iterator
  var currentRs:GsnResultSet=_
  override protected def createMetadata=new RowSetMetaDataImpl

  override def next:Boolean={
    if (currentRs!=null && currentRs.next)
      true
    else if (currentRs==null || iter.hasNext){ 
      currentRs=iter.next
      currentRs.next
    }
    else false
  }
  override def findColumn(columnLabel: String): Int = 
    currentRs.findColumn(columnLabel)
    
  
  override def getObject(columnIndex:Int):Object=  
    currentRs.getObject(columnIndex)
  
  override def getObject(columnLabel:String):Object=
    currentRs.getObject(columnLabel)
    
 override def getMetaData=if (currentRs!=null)currentRs.getMetaData
 else super.getMetaData
}
