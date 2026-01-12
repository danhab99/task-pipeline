package main

type Manifest struct {
	Steps []ManifestStep `toml:"step"`
}

type ManifestStep struct {
	Name     string `toml:"name"`
	Script   string `toml:"script"`
	Start    bool   `toml:"start"`
	Parallel *int   `toml:"parallel"`
}
