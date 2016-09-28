name := "adapter-esper"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.12"

libraryDependencies ++= Seq(
  "es.upm.fi.oeg.morph.streams" % "esper-engine" % "1.0.4",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "lsir-releases" at "http://osper.epfl.ch:8081/artifactory/gsn-release",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"  
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

