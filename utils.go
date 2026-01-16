package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

func makeTempFile() (string, error) {
	f, err := os.CreateTemp("/tmp", "*")
	if err != nil {
		return "", err
	}
	path := f.Name()

	// Close immediately; file still exists
	if err := f.Close(); err != nil {
		return "", err
	}

	return path, nil
}

func copyFileWithSHA256(src, dst string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	defer out.Close()

	hasher := sha256.New()

	// Write to BOTH the file and the hasher
	writer := io.MultiWriter(out, hasher)

	_, err = io.Copy(writer, in)
	if err != nil {
		return "", err
	}

	if err := out.Sync(); err != nil {
		return "", err
	}

	sum := hasher.Sum(nil)
	return fmt.Sprintf("%x", sum), nil
}

func hashFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hasher := sha256.New()

	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func hashStringSHA256(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}
