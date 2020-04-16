name := "untitled"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.2"
val breezeVersion = "0.13.2"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % breezeVersion,
  "org.scalanlp" %% "breeze-natives" % breezeVersion,
  "org.scalanlp" %% "breeze-viz" % breezeVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4",
  "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4",
  "graphframes" % "graphframes" % "0.2.0-spark2.0-s_2.11"
)