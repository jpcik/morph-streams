name := "sparql-stream"

organization := "es.upm.fi.oeg.morph"

version := "1.0.5"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.13",  
  "org.apache.jena" % "jena-core" % "2.11.0" intransitive,
  "org.apache.jena" % "jena-arq" % "2.11.0" intransitive,
  "org.apache.jena" % "jena-iri" % "1.0.0" intransitive,
  "xerces" % "xercesImpl" % "2.11.0" ,  
  "commons-lang" % "commons-lang" % "2.4",
  "org.scalatest" % "scalatest_2.10" % "2.0.RC1" % "test"
)

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
