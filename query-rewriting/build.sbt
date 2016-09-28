name := "query-rewriting"

organization := "es.upm.fi.oeg.morph"

version := "1.0.12"

libraryDependencies ++= Seq(
  "net.sf.jsqlparser" % "jsqlparser" % "0.7.0" intransitive,
  "ch.qos.logback" % "logback-classic" % "1.0.13" ,  
  "es.upm.fi.oeg.morph" % "morph-querygen" % "1.0.8" ,
  "com.typesafe.akka" %% "akka-actor" % "2.3.4" ,
  "es.upm.fi.oeg.morph" % "kyrie" % "0.18.2",    
  "org.antlr" % "antlr" % "3.2" intransitive,
  "org.antlr" % "antlr-runtime" % "3.2" intransitive,
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Seq(
  DefaultMavenRepository,
  "lsir-releases" at "http://osper.epfl.ch:8081/artifactory/gsn-release",
    "plord" at "http://homepages.cs.ncl.ac.uk/phillip.lord/maven",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

