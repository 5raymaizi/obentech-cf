package output

import (
	"compress/gzip"
	"io"
)

// Compress read data from src and write compress data into dst
func Compress(src io.Reader, dst io.Writer) error {
	writer := gzip.NewWriter(dst)
	defer writer.Close()

	_, err := io.CopyN(writer, src, 1024*1024)
	for err == nil {
		_, err = io.CopyN(writer, src, 1024*1024)
	}
	if err == io.EOF {
		return nil
	}
	return err
}
