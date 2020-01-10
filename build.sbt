name := "qnt-scala"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.5"

resolvers += "Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all"
libraryDependencies += "edu.ucar" % "netcdf" % "4.3.22"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0"

libraryDependencies +=  "org.scalanlp" %% "breeze" % "1.0"
//libraryDependencies += "org.scalanlp" %% "breeze-natives" % "1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"