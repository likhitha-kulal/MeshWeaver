# Wire Protocols Specification
1. UDP Datagrams: JSON / Binary Framed RPCs
2. TCP Task Streams: 4-Byte length prefix + Cloudpickle Payload
3. SHA-256 Checksum Validation & HMAC authentication

## 4. Gossip & Telemetry Protocol
- **Epidemic Dissemination**: Heartbeat frequency 5.0s
- **Node Eviction Policy**: Timeout after 15.0s without heartbeat
- **Payload**: Host CPU% and RAM% usage metrics
