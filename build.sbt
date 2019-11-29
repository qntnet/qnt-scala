name := "qnt"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"

resolvers += "Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all"
libraryDependencies += "edu.ucar" % "netcdf" % "4.3.22"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.1"

resolvers += "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
libraryDependencies += "org.scala-saddle" %% "saddle-core" % "1.3.+"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"