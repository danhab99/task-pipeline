package main

type Manifest struct {
	Tasks []Task `toml:"task"`
}

