package com.syntaxjockey.smr.namespace

class NamespaceException(message: String) extends Exception(message)

class VersionMismatch(val expected: Long, val found: Long) extends NamespaceException("Expected version %d, but found version %d".format(expected, found))

class InvalidPathException(message: String) extends NamespaceException(message)

class PatchFailedException(message: String) extends NamespaceException(message)

class NamespaceOperationFailed(message: String) extends NamespaceException(message)
