ThisBuild / scalaVersion     := "2.12.14"
ThisBuild / version          := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "scala-data-proc",
    scalaVersion := "2.12.14",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
      "org.apache.spark" %% "spark-sql" % "3.1.3" % "compile",
      "org.apache.spark" %% "spark-mllib" % "3.1.3"% "compile",
      "org.apache.spark" %% "spark-avro" % "3.1.3" % "compile",
      "com.databricks" %% "spark-xml" % "0.13.0" % "compile"
//      scalaTest % Test
    )
  )