name := "JX-Analysis"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "geospark" at "https://repo1.maven.org/maven2/org/datasyslab/"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
	"org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
	"org.apache.spark" %% "spark-graphx" % "2.2.0" % "provided",
	"org.apache.spark" %% "spark-mllib" % "2.2.0" % "provided",
	"com.esri.geometry" % "esri-geometry-api" % "2.2.2",
	"com.databricks" %% "spark-avro" % "3.2.0",
	"graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11" % "provided",
	"io.spray" %% "spray-json" % "1.3.2",
	"com.github.scopt" %% "scopt" % "3.3.0",
	"com.typesafe" % "config" % "1.3.1",
	"org.datasyslab" % "geospark" % "1.2.0",
	"org.datasyslab" % "geospark-sql_2.2" % "1.2.0",
	"com.github.kxbmap" %% "configs" % "0.2.5",
	"joda-time" % "joda-time" % "2.9.4",
	"org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
	"com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.3" % "test" withSources()
)

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}

parallelExecution in Test := false
