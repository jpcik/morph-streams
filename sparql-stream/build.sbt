name := "sparql-stream"

organization := "es.upm.fi.oeg.morph"

version := "1.0.1"

libraryDependencies ++= Seq(
  "xml-apis" % "xml-apis" % "1.3.04",
  "com.hp.hpl.jena" % "jena" % "2.6.4",
  "com.hp.hpl.jena" % "arq" % "2.8.8",
  "commons-lang" % "commons-lang" % "2.4",
  "org.scalatest" % "scalatest_2.9.1" % "1.7.2" % "test",
  "org.scalacheck" % "scalacheck_2.9.1" % "1.9" % "test"
)

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
