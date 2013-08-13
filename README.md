# cql-protocol

`cql-protocol` is a library which implements Apache Cassandra's native CQL protocol spec v1 and v2 (almost) completely in pure javascript.

## Building block for Apache Cassandra drivers

This library is intended to be used by driver authors(including myself), not by users.
You can use this library to build your own Apache Cassandra client or even pseudo-server for testitng.

## Getting started

Native protocol is implemented as duplex stream using node.js's new Stream API.
You just attach your connection to an instance of `NativeProtocol` class to send and receive messages.

## Built with

these awesome libraries:

* node-uuid
* ipaddr.js
* jsbn
* long

and

* node-tap (for testing)

## What't not included (yet)

* Message compression
* Serializing RESULT message

