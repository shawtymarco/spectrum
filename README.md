<p align="center">
  <img src="assets/go-gopher.png" alt="Go gopher" width="180">
</p>

<h1 align="center">Spectrum</h1>

<p align="center">A fast Minecraft Bedrock proxy with native-backend multiversion support.</p>

See the isolated [dual public transport candidate](docs/dual-transport.md) for
RakNet/NetherNet composition, dependency prerequisites and validation limits.

## Compatibility status

This isolated branch uses published native 2223 APIs and validates native 2225
through the candidate overrides documented above. The new dual-listener path
requires the accompanying gophertunnel fixes before it can use published pins.
Preview registries and Mojang-client validation remain outstanding.

The consumer supplies its explicit registry-aware `go-multiversion` catalogue;
Spectrum keeps the backend wire native. Automated transport tests do not expand
production version coverage or imply support for unlisted releases.

## 🚀 Usage

Pass registry-aware historical protocols to the public listener and keep
`SyncProtocol` disabled so backends always speak the current native model:

```go
opts := spectrumutil.DefaultOpts()
opts.Addr = ":19132"

proxy := spectrum.NewSpectrum(
	spectrumserver.NewStaticDiscovery("dragonfly:19142", ""),
	slog.Default(),
	opts,
	nil,
)

err := proxy.Listen(minecraft.ListenConfig{
	AcceptedProtocols: protocols,
	StatusProvider:    spectrumutil.NewStatusProvider("Example", "Spectrum"),
})
```

Native clients retain raw packet forwarding. Historical clients automatically
cross the bidirectional `minecraft.Protocol` conversion boundary, and the
backend connection request carries the public client's real protocol ID.

## Examples

Each example is a separate executable package. Run commands from the repository
root, such as `go run ./example/simple`, to preserve the examples' working-directory
behavior. The resource-pack example reads `./resource_packs` relative to that
working directory.

- [Simple proxy](example/simple/main.go)
- [Control API](example/api/main.go)
- [Resource packs](example/resource-packs/main.go)
- Animations: [smooth](example/animation/smooth/main.go),
  [fade](example/animation/fade/main.go), [ease](example/animation/ease/main.go)
- Transports: [Spectral](example/transport/spectral/main.go),
  [QUIC](example/transport/quic/main.go)

## 🔗 Dependencies

- [`shawtymarco/gophertunnel`](https://github.com/shawtymarco/gophertunnel)
  provides the native protocol model and the protocol-486 RakNet v10 transport.
- [`shawtymarco/go-multiversion`](https://github.com/shawtymarco/go-multiversion)
  provides historical wire, registry, item, form, and gameplay conversion.
- [`shawtymarco/spectrum-df`](https://github.com/shawtymarco/spectrum-df)
  connects native Spectrum streams to Dragonfly and preserves client protocol
  capabilities for chunk encoding.
- [`cooldogedev/spectral`](https://github.com/cooldogedev/spectral) provides the
  default multiplexed backend transport. QUIC is also available.

## 🙏 Credits

- [cooldogedev/Spectrum](https://github.com/cooldogedev/spectrum)
- [Sandertv/gophertunnel](https://github.com/Sandertv/gophertunnel)
- [df-mc/dragonfly](https://github.com/df-mc/dragonfly)
- [Go gopher](https://go.dev/blog/gopher) by Renee French, used under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
