package utils

import (
	"bytes"
	"errors"
	"io"
	"os"
)

func LineCount(r io.Reader) (uint64, error) {
	buf := make([]byte, 32*1024)
	count := uint64(0)
	lineSep := []byte{'\n'}
	for {
		c, err := r.Read(buf)
		count += uint64(bytes.Count(buf[:c], lineSep))
		switch {
		case err == io.EOF:
			return count, nil
		case err != nil:
			return count, err
		}
	}
}

func Exists(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, os.ErrNotExist)
}
