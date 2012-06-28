name := "query-rewriting"

organization := "es.upm.fi.oeg.morph"

version := "1.0.0"

scalaVersion := "2.9.1"

crossPaths := false

libraryDependencies ++= Seq(
  "xml-apis" % "xml-apis" % "1.3.04",
  "es.upm.fi.oeg.morph" % "morph-core" % "1.0.0",
  "es.upm.fi.oeg.morph" % "morph-querygen" % "1.0.0",
  "es.upm.fi.oeg.integration" % "semantic-streams-core" % "0.0.1",
  "org.apache.ws.commons.axiom" % "axiom-api" % "1.2.11",
  "org.apache.ws.commons.axiom" % "axiom-impl" % "1.2.11",  
  "org.scalatest" % "scalatest_2.9.1" % "1.7.2" % "test",
  "org.scalacheck" % "scalacheck_2.9.1" % "1.9" % "test"
)

resolvers ++= Seq(  
  DefaultMavenRepository,
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

scalacOptions += "-deprecation"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))

unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))