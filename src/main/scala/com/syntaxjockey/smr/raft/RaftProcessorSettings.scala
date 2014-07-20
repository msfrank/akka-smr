package com.syntaxjockey.smr.raft

import java.nio.file.Path

import scala.concurrent.duration.FiniteDuration

/**
 * 
 */
case class RaftProcessorSettings(minimumProcessors: Int,
                                 electionTimeout: RandomBoundedDuration,
                                 idleTimeout: FiniteDuration,
                                 maxEntriesBatch: Int,
                                 logDirectory: Path,
                                 logSnapshotModulo: Int)
