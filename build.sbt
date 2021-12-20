name := "spark_homework"

version := "0.1"

scalaVersion := "2.13.7"

val sparkVersion = "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"