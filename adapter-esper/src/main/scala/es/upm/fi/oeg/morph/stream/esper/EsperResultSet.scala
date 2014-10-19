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
import es.upm.fi.oeg.morph.stream.algebra.xpr.ConstantXpr

class EsperResultSet(override val records:Stream[Array[Object]], 
    override val metadata: Map[String, Xpr],override val queryVars:Seq[String]) extends StreamResultSet {

}