package com.syntaxjockey.smr

import org.scalatest.{WordSpecLike, BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers
import akka.remote.testkit.{MultiNodeSpecCallbacks, MultiNodeConfig}
import com.typesafe.config.ConfigFactory

trait SMRMultiNodeSpec extends MultiNodeSpecCallbacks with WordSpecLike with MustMatchers with BeforeAndAfterAll {
  override def beforeAll() = multiNodeSpecBeforeAll()
  override def afterAll() = multiNodeSpecAfterAll()
}

object SMRMultiNodeConfig extends MultiNodeConfig {
  commonConfig(ConfigFactory.load("multi-jvm.conf"))
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
}