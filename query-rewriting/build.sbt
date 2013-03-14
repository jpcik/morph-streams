name := "query-rewriting"

organization := "es.upm.fi.oeg.morph"

version := "1.0.3"

libraryDependencies ++= Seq(
  "xml-apis" % "xml-apis" % "1.3.04",
  "net.sf" % "jsqlparser" % "0.0.1",
  "es.upm.fi.dia.oeg.sparql" % "resultbindings" % "0.0.1",
  "commons-lang" % "commons-lang" % "2.4",
  "com.google.guava" % "guava" % "r09",
  "es.upm.fi.oeg.morph" % "morph-core" % "1.0.1",
  "es.upm.fi.oeg.morph" % "morph-querygen" % "1.0.1",
  "com.typesafe.akka" % "akka-actor" % "2.0.2",
  "org.apache.ws.commons.axiom" % "axiom-api" % "1.2.11",
  "org.apache.ws.commons.axiom" % "axiom-impl" % "1.2.11",  
  "junit" % "junit" % "4.7" % "test",
  "org.scalatest" % "scalatest_2.9.1" % "1.7.2" % "test",
  "org.scalacheck" % "scalacheck_2.9.1" % "1.9" % "test"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"    
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

