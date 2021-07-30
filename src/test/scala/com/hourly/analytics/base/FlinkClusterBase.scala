package com.hourly.analytics.base

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, Suite}

trait FlinkClusterBase extends BeforeAndAfter { this: Suite =>

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(1)
      .setNumberTaskManagers(1)
      .build
  )

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }
}
