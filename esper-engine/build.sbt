name := "esper-engine"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.1"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.7",
  "ch.qos.logback" % "logback-classic" % "1.0.9",  
  "com.espertech" % "esper" % "4.3.0",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test",
  "com.typesafe.akka" %% "akka-actor" % "2.1.2",
  "com.typesafe.akka" %% "akka-remote" % "2.1.2",
  "com.typesafe.akka" %% "akka-kernel" % "2.1.2"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"  
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

