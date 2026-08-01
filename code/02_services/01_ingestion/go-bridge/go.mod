module github.com/trading/arrow-bridge

go 1.24.5

require github.com/arrow-trade/go-arrow v0.0.0

require (
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/boombuler/barcode v1.1.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pquerna/otp v1.5.0 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.65.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
)

// The Arrow HFT SDK is vendored in-repo (third_party/go-arrow) so the image
// build is self-contained and reproducible. Relative replace — resolves
// relative to this go.mod both on the host and inside the Docker build.
replace github.com/arrow-trade/go-arrow => ./third_party/go-arrow
