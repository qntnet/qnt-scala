name := "qnt"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"

resolvers += "Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all"
libraryDependencies += "edu.ucar" % "netcdf" % "4.3.22"

//resolvers += "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0"

//
//libraryDependencies += "org.scala-saddle" %% "saddle-core" % "1.3.+"

libraryDependencies +=  "org.scalanlp" %% "breeze" % "1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"