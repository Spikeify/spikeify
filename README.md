# spikeify
A lightweight Java object mapper for Aerospike.

Spikeify is a thin wrapper around Aerospike's native Java client library. It has the following features:

 - Maps database records to your Java objects, so you can simply work with your own classes.
 - Has a simple command-chain API for quick one-line access to most functionality.
 - Does not try to abstract or hide a native client library, but rather extend it. All functions of the underlying client lib can be  used concurrently with Spikeify.

Read more about it in the [Basic Usage](https://github.com/Spikeify/spikeify/wiki/Basic-Usage).

Status: Spikeify is currently in Alpha, which means that API can still change.

What works: object-record mapping, basic synchronous commands (get, create, update, delete, query, add/append/prepend, automatic index creation) and transactions are working.

What is not available yet: async operations, custom field serializers, queries on float/double values...

This project uses the excellent [YourKit Java profiler](https://www.yourkit.com/features/).
