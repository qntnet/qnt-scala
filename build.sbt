name := "qnt-scala"
homepage := Some(url("https://github.com/qntnet/qnt-scala"))
startYear := Some(2020)
licenses += "MIT" -> url("https://opensource.org/licenses/MIT")
description := "Library for investing strategies development on quantnet.ai"

organization := "ai.quantnet"
organizationName := "QuantNet, LLC."
organizationHomepage := Some(url("http://quantnet.ai"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/qntnet/qnt-scala"),
    "scm:git@github.com:qntnet/qnt-scala.git"
  )
)
developers := List(
  Developer(
    id    = "dgolovin.quantnet",
    name  = "Dmitry Golovin",
    email = "golovin@quantneet.ai",
    url   = url("http://github.com/quantnet-golovin")
  )
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

version := "0.2"

scalaVersion := "2.13.1"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.5"

resolvers += "Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all"
libraryDependencies += "edu.ucar" % "netcdf" % "4.3.22"

// resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.1"

libraryDependencies +=  "org.scalanlp" %% "breeze" % "1.0"
//libraryDependencies += "org.scalanlp" %% "breeze-natives" % "1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
