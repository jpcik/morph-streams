name := "query-rewriting"

organization := "es.upm.fi.oeg.morph"

version := "1.0.10"

libraryDependencies ++= Seq(
  "net.sf" % "jsqlparser" % "0.0.1",
  "ch.qos.logback" % "logback-classic" % "1.0.9",  
  "es.upm.fi.oeg.morph" % "morph-core" % "1.0.5",
  "es.upm.fi.oeg.morph" % "morph-querygen" % "1.0.4",
  "com.typesafe.akka" %% "akka-actor" % "2.1.2",
  "es.upm.fi.oeg.morph" % "kyrie" % "0.18.2",    
  "junit" % "junit" % "4.7" % "test",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test"
)

resolvers ++= Seq(
  DefaultMavenRepository,
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
    "plord" at "http://homepages.cs.ncl.ac.uk/phillip.lord/maven",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

