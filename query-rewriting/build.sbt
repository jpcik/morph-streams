name := "query-rewriting"

organization := "es.upm.fi.oeg.morph"

version := "1.0.1"

scalaVersion := "2.9.1"

crossPaths := false

libraryDependencies ++= Seq(
  "xml-apis" % "xml-apis" % "1.3.04",
  "net.sf" % "jsqlparser" % "0.0.1",
  "es.upm.fi.dia.oeg.sparql" % "resultbindings" % "0.0.1",
  "commons-lang" % "commons-lang" % "2.4",
  "com.google.guava" % "guava" % "r09",
  "es.upm.fi.oeg.morph" % "morph-core" % "1.0.1",
  "es.upm.fi.oeg.morph" % "morph-querygen" % "1.0.1",
  "org.apache.ws.commons.axiom" % "axiom-api" % "1.2.11",
  "org.apache.ws.commons.axiom" % "axiom-impl" % "1.2.11",  
  "org.scalatest" % "scalatest_2.9.1" % "1.7.2" % "test",
  "org.scalacheck" % "scalacheck_2.9.1" % "1.9" % "test"
)

resolvers ++= Seq(
  DefaultMavenRepository,
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "Local ivy Repository" at "file://"+Path.userHome.absolutePath+"/.ivy2/local"
)

scalacOptions += "-deprecation"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))

unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))

publishTo := Some(Resolver.file("jpc repo",new File(Path.userHome.absolutePath+"/git/jpc-repo/repo")))

publishMavenStyle := true