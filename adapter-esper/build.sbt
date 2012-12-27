name := "adapter-esper"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.2"

libraryDependencies ++= Seq(
  "es.upm.fi.oeg.morph" % "query-rewriting" % "1.0.1",
  "es.upm.fi.oeg.morph.streams" % "esper-engine" % "1.0.0",
  "com.espertech" % "esper" % "4.3.0",
  "org.scalatest" % "scalatest_2.9.1" % "1.7.2" % "test",
  "org.scalacheck" % "scalacheck_2.9.1" % "1.9" % "test",
  "com.typesafe.akka" % "akka-actor" % "2.0.2",
  "com.typesafe.akka" % "akka-remote" % "2.0.2",
  "com.typesafe.akka" % "akka-kernel" % "2.0.2"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"  
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

