name := "sparql-stream"

organization := "es.upm.fi.oeg.morph"

version := "1.0.6"

libraryDependencies ++= Seq(
  "es.upm.fi.oeg.morph" % "morph-core" % "1.0.7" ,
  "commons-lang" % "commons-lang" % "2.4",
  "ch.qos.logback" % "logback-classic" % "1.1.1" % "test",      
  "org.scalatest" %% "scalatest" % "2.2.1" % "test")

resolvers ++= Seq(
  "lsir-releases" at "http://osper.epfl.ch:8081/artifactory/gsn-release"
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
