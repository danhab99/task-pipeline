package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"syscall"
)

func checkDiskSpace(dbPath string) {
	stat := syscall.Statfs_t{}
	err := syscall.Statfs(dbPath, &stat)
	if err != nil {
		return // Silent fail if we can't check
	}

	availableGB := float64(stat.Bavail) * float64(stat.Bsize) / (1024 * 1024 * 1024)
	totalGB := float64(stat.Blocks) * float64(stat.Bsize) / (1024 * 1024 * 1024)
	usedGB := totalGB - availableGB
	percentUsed := (usedGB / totalGB) * 100

	if percentUsed > 85 {
		mainLogger.Printf("⚠️  Disk %s is %.1f%% full (%.1fGB free / %.1fGB total). This may cause database slowness.\n", dbPath, percentUsed, availableGB, totalGB)
	}
}

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

func mkTemp() string {
	dir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}

	return dir
}
