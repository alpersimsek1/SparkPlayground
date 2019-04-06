name := "Test"

version := "0.1.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

assemblyMergeStrategy in assembly :=
  {
    //    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }

libraryDependencies ++= Seq (
  "org.apache.spark"      %% "spark-core"         % sparkVersion,//% "provided",
  "org.apache.spark"      %% "spark-sql"          % sparkVersion,// % "provided",
  "org.apache.spark"      %% "spark-streaming"    % sparkVersion,// % "provided",
  "org.apache.spark"      %% "spark-hive"         % sparkVersion,// % "provided",
  "org.apache.spark"      %% "spark-mllib"        % sparkVersion,// % "provided",
  "org.apache.hadoop"     %  "hadoop-common"      % "2.7.7",
  "org.apache.hadoop"     % "hadoop-aws"          % "2.7.7",
  "io.spray" %%  "spray-json" % "1.3.5"
  
)
