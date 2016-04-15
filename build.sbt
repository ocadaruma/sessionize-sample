name := "sessionize-sample"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % Provided,
  "com.github.nscala-time" %% "nscala-time" % "2.6.0")
