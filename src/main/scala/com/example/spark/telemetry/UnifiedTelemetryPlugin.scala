package com.example.spark.telemetry

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}

final class UnifiedTelemetryPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new TelemetryDriverPlugin
  override def executorPlugin(): ExecutorPlugin = new TelemetryExecutorPlugin
}
