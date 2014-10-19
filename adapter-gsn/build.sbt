
name := "adapter-gsn"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.10"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.jibx" % "jibx-bind" % "1.2.2",
  "commons-collections" % "commons-collections" % "3.2.1",
  "ch.epfl.lsir" % "gsn" % "1.1.2"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",  
  "aldebaran-libs" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-external-libs-local"  
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
