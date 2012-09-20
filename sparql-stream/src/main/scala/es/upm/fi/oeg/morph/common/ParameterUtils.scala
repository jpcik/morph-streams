package es.upm.fi.oeg.morph.common

import java.util.Properties
import java.io.InputStream
import scala.io.Source
import java.io.File
import java.io.FileInputStream
import java.net.URL

object ParameterUtils {
  private val COMMENT_CHAR = "#"
  def load(fis:InputStream):Properties ={
    val props = new Properties
    props.load(fis)
    fis.close
    return props
  }
  def load(propsFile:File):Properties= 
    load(new FileInputStream(propsFile))     
  
    
  def loadAsString(url:URL):String=  {
    val fr = Source.fromFile(url.toURI())
    fr.getLines.filterNot(_.startsWith(COMMENT_CHAR)).mkString("\n")
  }
    
  def loadQuery(path:String):String=
    loadAsString(this.getClass.getClassLoader().getResource(path))		
  
	
}