package output

import (
	"encoding/csv"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
)

type (
	CSVStream struct {
		Fields []string
		Path   string
		Create time.Time
		fp     io.WriteCloser
		cw     *csv.Writer
	}
)

func NewStream(fields []string, path string) *CSVStream {
	f := make([]string, len(fields))
	copy(f, fields)
	return &CSVStream{
		Fields: f,
		Path:   path,
	}
}

func (s *CSVStream) Open() error {
	_, err := os.Stat(s.Path)
	if err != nil && !os.IsNotExist(err) {
		return errors.WithMessagef(err, "stat fail '%s'", s.Path)
	}

	var (
		file io.WriteCloser
		cw   *csv.Writer
	)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(s.Path, os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			return errors.WithMessagef(err, "create fail '%s'", s.Path)
		}
		os.Chmod(s.Path, 0766) // -rwxrw-rw-
		cw = csv.NewWriter(file)
		if err := cw.Write(s.Fields); err != nil {
			return errors.WithMessagef(err, "write fields fail")
		}

	} else {
		file, err = os.OpenFile(s.Path, os.O_APPEND|os.O_RDWR, 0777)
		if err != nil {
			return errors.WithMessagef(err, "open fail '%s'", s.Path)
		}
		os.Chmod(s.Path, 0766)
		cw = csv.NewWriter(file)
	}

	s.fp = file
	s.cw = cw
	s.Create = time.Now()
	return nil
}

func (s *CSVStream) Write(data map[string]string) error {
	row := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		v, ok := data[f]
		if !ok {
			return errors.Errorf("missing field '%s'", f)
		}
		row[i] = v
	}
	return s.cw.Write(row)
}

func (s *CSVStream) Close() error {
	s.cw.Flush()
	file := s.fp.(*os.File)
	if err := file.Sync(); err != nil {
		return err
	}
	return s.fp.Close()
}
