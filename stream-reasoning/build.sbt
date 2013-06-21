name := "stream-reasoning"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.1"

libraryDependencies ++= Seq(
  //"es.upm.fi.oeg.morph" % "query-rewriting" % "1.0.5",
  "es.upm.fi.oeg.morph.streams" % "adapter-esper" % "1.0.5" ,
  "es.upm.fi.oeg.morph.streams" % "esper-engine" % "1.0.1",
  "es.upm.fi.oeg.morph" % "kyrie" % "0.15.3",  
  "com.espertech" % "esper" % "4.9.0",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test",
  "com.typesafe.akka" %% "akka-actor" % "2.1.2",
  "com.typesafe.akka" %% "akka-remote" % "2.1.2",
  "com.typesafe.akka" %% "akka-kernel" % "2.1.2"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "nightlabs" at "http://dev.nightlabs.org/maven-repository/repo",
  "jmora" at "https://dl.dropboxusercontent.com/u/452942/maven"
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")





