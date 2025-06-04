# fastpacketconn

A Go library for writing fast UDP clients/servers using offload techniques.

## Offloads

### Linux

#### UDP GRO/GSO (Kernel v5.x and later)

For optimal performance, you will probably need to increase the maximum receive 
and send buffer sizes. The defaults for UDP are often too low for high-throughput 
applications.

```shell
sysctl -w net.core.rmem_max=7500000
sysctl -w net.core.wmem_max=7500000
```