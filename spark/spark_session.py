"""
Spark Session Factory with Delta Lake and S3A/MinIO Support.
Configured for Windows & Linux environments with Java 17/21/23 compatibility.
"""
import os
import sys
from pathlib import Path

# Automatically configure HADOOP_HOME for Windows using project-local winutils
base_dir = Path(__file__).resolve().parent.parent
hadoop_dir = base_dir / "hadoop"
if hadoop_dir.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["hadoop.home.dir"] = str(hadoop_dir)
    os.environ["PATH"] = f"{hadoop_dir / 'bin'};" + os.environ["PATH"]

# Automatically prefer JDK 17 (recommended LTS for PySpark 3.5) if available
if "JAVA_HOME" not in os.environ or "jdk-23" in os.environ.get("JAVA_HOME", ""):
    jdk17_path = Path("C:/Program Files/Java/jdk-17")
    if jdk17_path.exists():
        os.environ["JAVA_HOME"] = str(jdk17_path)
        os.environ["PATH"] = f"{jdk17_path}/bin;" + os.environ["PATH"]

# Explicitly configure Python executable path for PySpark driver & workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Add Java options for Java 17+ compatibility with PySpark and Hadoop
JAVA_OPTS = (
    "-Djava.security.manager=allow "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)
os.environ["PYSPARK_SUBMIT_ARGS"] = f'--driver-java-options="{JAVA_OPTS}" pyspark-shell'

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(
    app_name: str = "TikiMarketPlatform",
    master: str = "local[*]",
    use_delta: bool = True,
    use_minio: bool = False,
) -> SparkSession:
    """
    Build and return a configured SparkSession.
    Supports local filesystem storage or S3A/MinIO lakehouse.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.python.use.daemon", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.extraJavaOptions", JAVA_OPTS)
        .config("spark.executor.extraJavaOptions", JAVA_OPTS)
    )

    if use_delta:
        builder = (
            builder.config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

    if use_minio:
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        minio_access = os.getenv("MINIO_ACCESS_KEY", "admin")
        minio_secret = os.getenv("MINIO_SECRET_KEY", "admin12345")

        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    if use_delta:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
