# spikeify
A lightweight Java object mapper for Aerospike.

Spikeify is a thin wrapper around Aerospike's native Java client library. It has the following features:

 - Maps database records to your Java objects, so you can simply work with your own classes.
 - Has a simple command-chain API for quick one-line access to most functionality.
 - Does not try to abstract or hide a native client library, but rather extend it. All functions of the underlying client lib can be  used concurrently with Spikeify.

Read more about it in the [Wiki](https://github.com/Spikeify/spikeify/wiki).

