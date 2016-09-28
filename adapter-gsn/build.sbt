
name := "adapter-gsn"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.11"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.jibx" % "jibx-bind" % "1.2.2",
  "commons-collections" % "commons-collections" % "3.2.1"
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
