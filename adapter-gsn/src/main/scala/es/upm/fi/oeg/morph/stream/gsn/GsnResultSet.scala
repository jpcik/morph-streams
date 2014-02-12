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
import es.upm.fi.oeg.morph.stream.algebra.xpr.ConstantXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.NullValueXpr

class GsnResultSet(override val records: Stream[Array[Object]], override val metadata: Map[String, Xpr]) 
  extends StreamResultSet {
  override val queryVars=(metadata.map(m=>m._2.varNames).flatten.toSet.filterNot(_.equals("timed"))
    .toList++List("timed")).toSeq
  val logger=LoggerFactory.getLogger(this.getClass)
  
  override def getObject(columnLabel:String):Object={
    
    metadata(columnLabel) match{
      case rep:ReplaceXpr=>
        println(internalLabels)
        rep.evaluate(internalLabels.map(l=>l._1->current(l._2)).toMap)
      case v:VarXpr=>current(internalLabels(v.varName))
      case c:ConstantXpr=>c.evaluate
      case NullValueXpr=>null
    }
  }
}
