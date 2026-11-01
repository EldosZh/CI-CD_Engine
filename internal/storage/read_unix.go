//go:build !windows

package storage

import "os"

func readMetadata(path string) ([]byte, error) { return os.ReadFile(path) }
