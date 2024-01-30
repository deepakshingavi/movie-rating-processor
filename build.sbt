val sparkVersion = "3.3.3"

lazy val root = (project in file("."))
  .settings(
      name := "movie-rating-processor",
      version := "1.0",
      scalaVersion := "2.12.18",

      // Add Spark dependencies
      libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
      libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
      libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      libraryDependencies += "com.holdenkarau" %% "spark-testing-base" %  "3.3.1_1.4.0" % Test,
      libraryDependencies += "org.apache.hadoop" % "hadoop-client-api" % "3.3.2"
  )
