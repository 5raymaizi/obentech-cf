package reader

import (
	"cf_arbitrage/exchange"
	"compress/gzip"
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"time"

	ccexgo "github.com/NadiaSama/ccexgo/exchange"
	"github.com/pkg/errors"
)

type (
	//GzDepthReader 从gz压缩包读取depth数据
	GzDepthReader interface {
		Read() (*exchange.Depth, error)
		Close() error
	}

	gzDepthReader struct {
		file       *os.File
		gzipReader *gzip.Reader
		csvReader  *csv.Reader
		depthLen   int
	}
)

func NewGzDepthReader(path string) (GzDepthReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.WithMessage(err, "open file fail")
	}
	reader, err := gzip.NewReader(file)
	if err != nil {
		return nil, errors.WithMessage(err, "create gzip reader fail")
	}

	csvReader := csv.NewReader(reader)
	//skip first line
	fields, err := csvReader.Read()
	if err != nil {
		return nil, errors.WithMessage(err, "read csv file fail")
	}

	if (len(fields)-2)%4 != 0 {
		return nil, errors.Errorf("invalid csv header %s", strings.Join(fields, ","))
	}

	return &gzDepthReader{
		file:       file,
		csvReader:  csvReader,
		gzipReader: reader,
		depthLen:   (len(fields) - 2) / 4,
	}, nil

}

func (zdr *gzDepthReader) Read() (*exchange.Depth, error) {
	fields, err := zdr.csvReader.Read()
	if err != nil {
		return nil, err
	}

	idx := 0
	readField := func() string {
		ret := fields[idx]
		idx += 1
		return ret
	}

	asks := make([]ccexgo.OrderElem, zdr.depthLen)
	bids := make([]ccexgo.OrderElem, zdr.depthLen)
	for _, dst := range [][]ccexgo.OrderElem{asks, bids} {
		for i := 0; i < zdr.depthLen; i++ {
			amount, _ := strconv.ParseFloat(readField(), 64)
			price, _ := strconv.ParseFloat(readField(), 64)
			dst[i] = ccexgo.OrderElem{
				Amount: amount,
				Price:  price,
			}
		}
	}

	id, _ := strconv.ParseUint(readField(), 10, 64)

	field := readField()
	var (
		ts  time.Time
		fmt string
	)
	switch len(field) {
	case 29:
		fmt = "2006-01-02 15:04:05 -0700 MST"

	case 31:
		fmt = "2006-01-02 15:04:05.0 -0700 MST"

	case 32:
		fmt = "2006-01-02 15:04:05.00 -0700 MST"

	case 33:
		fmt = "2006-01-02 15:04:05.000 -0700 MST"
	}
	ts, err = time.Parse(fmt, field)
	if err != nil {
		return nil, errors.WithMessage(err, "parse timestamp failed")
	}

	return &exchange.Depth{
		ID: id,
		OrderBook: &ccexgo.OrderBook{
			Bids:    bids,
			Asks:    asks,
			Created: ts,
		},
	}, nil
}

func (zdr *gzDepthReader) Close() error {
	if err := zdr.file.Close(); err != nil {
		return errors.WithMessage(err, "close file fail")
	}
	if err := zdr.gzipReader.Close(); err != nil {
		return errors.WithMessage(err, "close zip reader fail")
	}
	return nil
}
