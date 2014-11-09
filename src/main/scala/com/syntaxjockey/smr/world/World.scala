package com.syntaxjockey.smr.world

import akka.util.ByteString
import com.syntaxjockey.smr.Configuration
import scala.util.Try

import com.syntaxjockey.smr.namespace.{Node, Path}

/**
 *
 */
trait World {

  def version: Long

  def configuration: Configuration

  def getNode(path: Path): Node

  def putNode(path: Path, node: Node): Try[Unit]

  def deleteNode(path: Path): Try[Unit]

  def getData(path: Path): ByteString

  def putData(path: Path, data: ByteString): Try[Unit]

}
