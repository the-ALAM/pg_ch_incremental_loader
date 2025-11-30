#!/bin/bash

# One-click Apache Spark installer for Ubuntu

set -e

echo "Updating package lists..."
sudo apt-get update

echo "Installing Java OpenJDK 11..."
sudo apt-get install -y openjdk-11-jdk

echo "Installing other dependencies (wget, tar, curl)..."
sudo apt-get install -y wget curl tar

SPARK_VERSION="4.0.1"
HADOOP_VERSION="3"
SPARK_ARCHIVE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
SPARK_TGZ="${SPARK_ARCHIVE}.tgz"
SPARK_DOWNLOAD_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"

echo "Downloading Apache Spark $SPARK_VERSION with Hadoop $HADOOP_VERSION support..."
wget -q "${SPARK_DOWNLOAD_URL}" -O "/tmp/${SPARK_TGZ}"

echo "Extracting Spark..."
sudo tar -xzf "/tmp/${SPARK_TGZ}" -C /opt
sudo ln -sfn /opt/${SPARK_ARCHIVE} /opt/spark

echo "Setting environment variables..."
SPARK_PROFILE=/etc/profile.d/spark.sh
sudo tee $SPARK_PROFILE > /dev/null <<EOF
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
export SPARK_HOME=/opt/spark
export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOF

source $SPARK_PROFILE

echo "Installing findspark for Python (optional, for pyspark usage)..."
if command -v python3 &> /dev/null; then
  python3 -m pip install --user --upgrade pip
  python3 -m pip install --user findspark pyspark
fi

echo "Spark installation complete!"
echo ""
echo "To start using Spark, either open a new shell session or run:"
echo "  source /etc/profile.d/spark.sh"
echo "To start the Spark shell, run:"
echo "  spark-shell"
echo "To start PySpark, run:"
echo "  pyspark"
echo ""
echo "For Spark standalone master/worker:"
echo "  start-master.sh"
echo "  start-worker.sh spark://<your-host>:7077"
