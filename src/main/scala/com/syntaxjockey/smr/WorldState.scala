package com.syntaxjockey.smr

import com.syntaxjockey.smr.namespace.Namespace

/**
 *
 */
case class WorldState(version: Long, namespaces: Map[String,Namespace])
