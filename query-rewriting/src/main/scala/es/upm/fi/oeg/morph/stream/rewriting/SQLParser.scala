package es.upm.fi.oeg.morph.stream.rewriting
import net.sf.jsqlparser.parser.CCJSqlParserManager
import java.io.StringReader
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.PlainSelect
import collection.JavaConversions._
import net.sf.jsqlparser.statement.select.SelectExpressionItem
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.BinaryExpression

object SQLParser {
  private def statement(query:String)=new CCJSqlParserManager().parse(new StringReader(query))
  
  def tableAlias(query: String): String={
    val s = statement(query)
    val select = s.asInstanceOf[Select]
    val ps = select.getSelectBody.asInstanceOf[PlainSelect]
    val tab = ps.getFromItem.asInstanceOf[net.sf.jsqlparser.schema.Table]
    tab.getName    
  }
  def projections(query:String)={
    val s=statement(query)
    val select=s.asInstanceOf[Select]
    val proj=select.getSelectBody.asInstanceOf[PlainSelect].getSelectItems()
    proj.map{p=>
      val item=p.asInstanceOf[SelectExpressionItem]
      val alias=if (item.getAlias==null) item.getExpression else item.getAlias
      alias->item.getExpression
    }.toMap
  }
  
  def selections(query:String)={
    val select=statement(query).asInstanceOf[Select]
    val where=select.getSelectBody.asInstanceOf[PlainSelect].getWhere
    if (where!=null)
      Some(expressions(where))
    else None
  }
  
  private def expressions(e:Expression):Array[(String,String,String)]=e match{
    case bi:AndExpression=>
//      (bi.getLeftExpression,bi.getStringExpression,bi.getRightExpression) match {
//        case (l,"and",r)=>
          expressions(bi.getLeftExpression)++expressions(bi.getRightExpression)
 
    //}
    case eq:EqualsTo=>Array((eq.getLeftExpression.toString,eq.getStringExpression,eq.getRightExpression.toString))  
    case _=> throw new Exception("Expression not supported "+e) }
}