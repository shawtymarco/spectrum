<p align="center">
  <img src="assets/go-gopher.png" alt="Go gopher" width="180">
</p>

<h1 align="center">Spectrum</h1>

<p align="center">A fast Minecraft Bedrock proxy with native-backend multiversion support.</p>

## ✅ Supported Versions

This maintained fork keeps the backend wire native and accepts the verified
`go-multiversion` adapters below.

| Protocol ID | Minecraft version | Path | BRBW automated E2E |
|------------:|-------------------|------|:------------------:|
| 2169 | 1.26.45 | Native gophertunnel | ✅ |
| 2168 | 1.26.40-1.26.44 | `go-multiversion/v1_26_44` | ✅ |
| 1001 | 1.26.30-1.26.34, 1.26.36 | `go-multiversion/v1_26_30` | ✅ |
| 975 | 1.26.20, 1.26.21, 1.26.23 | `go-multiversion/v1_26_20` | ✅ |
| 844 | 1.21.110-1.21.114 | `go-multiversion/v1_21_110` | ✅ |
| 827 | 1.21.100-1.21.102 | `go-multiversion/v1_21_100` | ✅ |
| 486 | 1.18.10-1.18.12 | `go-multiversion/v1_18_10` | ✅ |

> [!NOTE]
> Version coverage is explicit. Unlisted releases and previews are not implied.

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
