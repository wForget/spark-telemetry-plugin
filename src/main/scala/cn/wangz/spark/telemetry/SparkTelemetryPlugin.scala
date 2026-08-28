package cn.wangz.spark.telemetry

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}

final class SparkTelemetryPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new TelemetryDriverPlugin
  override def executorPlugin(): ExecutorPlugin = new TelemetryExecutorPlugin
}
