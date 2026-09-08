module github.com/antst/sessionbus

go 1.24

require github.com/antst/sessionbus/bus/sdk/go v0.1.0-pre.2

require golang.org/x/sys v0.30.0 // indirect

replace github.com/antst/sessionbus/bus/sdk/go => ./bus/sdk/go
