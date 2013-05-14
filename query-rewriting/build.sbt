name := "query-rewriting"

organization := "es.upm.fi.oeg.morph"

version := "1.0.5"

libraryDependencies ++= Seq(
  "net.sf" % "jsqlparser" % "0.0.1",
  "ch.qos.logback" % "logback-classic" % "1.0.9",  
  "es.upm.fi.oeg.morph" % "morph-core" % "1.0.3",
  "es.upm.fi.oeg.morph" % "morph-querygen" % "1.0.3",
  "com.typesafe.akka" %% "akka-actor" % "2.1.2",
  "org.apache.ws.commons.axiom" % "axiom-api" % "1.2.11",
  "org.apache.ws.commons.axiom" % "axiom-impl" % "1.2.11",  
  "junit" % "junit" % "4.7" % "test",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"    
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

