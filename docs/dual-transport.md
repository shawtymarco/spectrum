# Dual public transport candidate

This branch implements one Spectrum owner with RakNet and NetherNet public
listeners. It is a development candidate, not a production client-support claim.
The accompanying gophertunnel branch is required: it fixes zero-value RakNet
construction and joins listener connection cleanup. Until that fork is published,
use a disposable modfile; never commit a local path or unpublished dependency pin.
The public pin is `4ea1495a39f2` (native 2223); candidate validation uses native
2225 at `4d6bffd4ed49ac9298d6abaa329415432a16f3e4` (based on `8fe51a21de6d`). Plain tests of the new dual
factories require that override until the fixed dependency can be published.

## Entry and ownership

`Listen(config)` keeps the single-RakNet API. `Listener()` returns the first
listener. `ListenWith(config, factories...)` supplies one shared ListenerGroup
and policy, adopts successful results, and rolls them all back if any bind fails.
`Listeners()` returns a copy. Factories must preserve the supplied authentication,
protocol, status and pack policies and undo their own failed bind.

```go
err := proxy.ListenWith(config,
    frontend.RakNet(raknetAddress, limits),
    frontend.NetherNet(frontend.NetherNetConfig{
        SignalingAddress: signalingAddress,
        ICEAddress: iceAddress,
        Identity: persistentOperatorKey,
        PublicIPs: publicIPs,
        TLS: tlsConfig,
        Limits: limits,
    }),
)
```

The helper uses go-nethernet v1.0.24's Handler, WebRTC API, callbacks and identity
issuer. TCP HTTP/S signaling and the fixed ICE UDP mux have separate addresses;
ICE must not share RakNet's socket. `PublicIPs` supports 1:1 NAT with the same UDP
port. Arbitrary NAT port translation, TURN provisioning, Xbox discovery/provider
registration and external reachability are outside this helper. Xbox authentication
still occurs in gophertunnel's Login. The operator loads and backs up a P-384 key;
the helper never invents one. TLS and operator identity are separate keys.
Anonymous mode requires disabled Minecraft authentication and is used by offline tests.

Defaults are 64 signaling sockets/requests, 10-second negotiation, 128 pending
Minecraft bootstraps per transport, 10-second authentication and 2-minute total
pack/bootstrap delivery. These differ from Spectrum's one-minute backend/spawn
budget. HTTP status reads the shared count per request; RakNet retains its periodic
pong refresh. MaximumMTU applies only to RakNet. Wrappers forward frame headers,
packet reads, encryption, latency, legacy protocol version, context and key checks.

All accepted connections, including pre-backend sessions, belong to Spectrum's
shutdown owner. Close stops admission, closes listeners, cancels sessions and
joins accepts, auto-login and session cleanup. gophertunnel joins pre-auth readers
and the pong updater. RakNet's reliable close drain may take approximately five
seconds. With AutoLogin disabled, callers must join their own initialization work;
BRBW does this through its admission gate.

Successful registration replaces only an older login generation. Failed or delayed
older logins cannot erase the current route. BRBW admission/lease policy still runs
before LoginContext; transport IDs are never account IDs.

## Backend boundary

Transport and Minecraft protocol are independent. The accepted Conn selects its
adapter; `SyncProtocol=false` keeps the private backend native. Transfers retain
the public Conn. SpectrumDF must understand `nethernet://<network>/<connection>`
in the existing ConnectionRequest address string before NetherNet is enabled.
Wire fields are unchanged; RakNet keeps its UDP address. Neither this URI nor
signaled ICE candidates may become an IP correlation hash.

## Sources and deliberate differences

Reviewed WDPE master `aac47b18e29d8977dc613da9f39b632b8832bc80` and its branch/PR
inventory (release head `2dfd9fa83356c990aa30c76bd55866db11cec819`). Code evidence
comes from master: `NetherNetInterface`, `TransportProfile`,
`NetherNetServerSessionInitializer`, `LoginUpstreamHandler`, `NetherNetServerInfo`
and `ProxiedPlayer`, beneath `src/main/java/dev/waterdog/waterdogpe/`.

- [PR 463](https://github.com/WaterdogPE/WaterdogPE/pull/463) is merged at
  `cca3bb256771323c1bd2f541f7886d845b6fe158`; reviewed head
  `3f0c9b8c9e700a55ab7c29a3c4b1ba70fb5f5f4b`. It provides parallel public
  interfaces, common Bedrock sessions/login and transport profiles.
- [PR 470](https://github.com/WaterdogPE/WaterdogPE/pull/470), head
  `78b41819bf91d733a6cb4148cd3b75c77cea0adf`, remained open. Its Xbox original-login
  forwarding to selected NetherNet BDS backends is not implemented here.
- [PR 471](https://github.com/WaterdogPE/WaterdogPE/pull/471), head
  `1129ac4dd42bb991fd662d417fa4cf7e9f55f163`, remained open. Forced-host signaling
  routing differs from Spectrum's configured backend discovery.

WDPE establishes ordinary Bedrock backend connections. Spectrum keeps its framed
private protocol and BRBW's mTLS QUIC/SpectrumDF route. WDPE's inspected interface
leaves RakNet available after a NetherNet startup failure; this candidate rolls
back its required listener set. No GPL WDPE implementation code was copied.

Mojang's [26.50.24 notes](https://feedback.minecraft.net/hc/en-us/articles/47907593889677-Minecraft-Beta-Preview-26-50-24)
make NetherNet the dedicated-server default. The
[26.60.22/23 notes](https://feedback.minecraft.net/hc/en-us/articles/48740748263565-Minecraft-Beta-Preview-26-60-22-23)
replace the BDS RakNet deprecation warning with an error. These establish the BDS
transition, not removal on every Xbox/client/platform/build.

## Verification boundary

Candidate tests/vet and WSL race checks exercise real RakNet and HTTP/WebRTC,
concurrent admission, shared count/limit/status, duplicate identity replacement,
backend movement, initialization cancellation, real bind rollback and socket
reclamation. Wrapper tests cover transport capabilities and key mismatch. BRBW
also tests native 2225 and outgoing 2193 through both public transports and its
private mTLS QUIC/SpectrumDF visibility-transfer fixture.

Fixtures are offline clients and existing registries. They do not validate Xbox
credentials, Mojang clients, internet/NAT reachability, public regional Transfer,
Preview registry semantics or deployment. The .29 bundle remains native2223; no
.30 data was extracted/substituted. Publish dependencies, re-pin consumers, finish
native-data and real-client validation, then review enablement separately.
