name := "sparql-stream"

organization := "es.upm.fi.oeg.morph"

version := "1.0.4"

libraryDependencies ++= Seq(
  "org.apache.jena" % "jena-core" % "2.10.0" intransitive,
  "org.apache.jena" % "jena-arq" % "2.10.0" intransitive,
  "xerces" % "xercesImpl" % "2.10.0" ,  
  //"xml-apis" % "xml-apis" % "1.3.04",
  "commons-lang" % "commons-lang" % "2.4",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test"
)

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
