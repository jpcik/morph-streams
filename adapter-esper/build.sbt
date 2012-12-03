name := "adapter-esper"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.1"

scalaVersion := "2.9.1"

crossPaths := false

libraryDependencies ++= Seq(
  "es.upm.fi.oeg.morph" % "query-rewriting" % "1.0.0",
  "com.espertech" % "esper" % "4.3.0",
  "org.scalatest" % "scalatest_2.9.1" % "1.7.2" % "test",
  "org.scalacheck" % "scalacheck_2.9.1" % "1.9" % "test",
  "com.typesafe.akka" % "akka-actor" % "2.0.2",
  "com.typesafe.akka" % "akka-remote" % "2.0.2",
  "com.typesafe.akka" % "akka-kernel" % "2.0.2"
)

resolvers ++= Seq(
  DefaultMavenRepository,
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "aldebaran-snapshots" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-snapshots-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"  
)

scalacOptions += "-deprecation"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))

unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))
