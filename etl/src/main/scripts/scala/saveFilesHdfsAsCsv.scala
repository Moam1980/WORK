import sa.com.mobily.hdfs.spark.FileHdfsDsl
import sa.com.mobily.utils.EdmCoreUtils

sc.parallelize(FileHdfsDsl.toFilesHdfs("${HDFS_HOME}")).map(file =>
  file.fields.mkString(EdmCoreUtils.Separator)).saveAsTextFile("${HDFS_FILES_METRICS_PATH}")
