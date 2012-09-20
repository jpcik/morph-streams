name := "sparql-stream"

organization := "es.upm.fi.oeg.morph"

version := "1.0.0"

scalaVersion := "2.9.1"

crossPaths := false

libraryDependencies ++= Seq(
  "xml-apis" % "xml-apis" % "1.3.04",
  "com.hp.hpl.jena" % "jena" % "2.6.4",
  "com.hp.hpl.jena" % "arq" % "2.8.8",
  "commons-lang" % "commons-lang" % "2.4",
  "org.scalatest" % "scalatest_2.9.1" % "1.7.2" % "test",
  "org.scalacheck" % "scalacheck_2.9.1" % "1.9" % "test"
)

resolvers ++= Seq(
  DefaultMavenRepository,
  "Local ivy Repository" at "file://"+Path.userHome.absolutePath+"/.ivy2/local"
)

scalacOptions += "-deprecation"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

//unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))

//unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))

publishTo := Some(Resolver.file("jpc repo",new File(Path.userHome.absolutePath+"/git/jpc-repo/repo")))

publishMavenStyle := true