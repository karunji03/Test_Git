package io.prophecy.pipelines.test_git_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.test_git_pipeline.config._
import io.prophecy.pipelines.test_git_pipeline.functions.UDFs._
import io.prophecy.pipelines.test_git_pipeline.functions.PipelineInitCode._
import io.prophecy.pipelines.test_git_pipeline.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {
  def apply(context: Context): Unit = {}

  def main(args:     Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Test_Git_Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/Test_Git_Pipeline")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/Test_Git_Pipeline") {
      apply(context)
    }
  }

}
