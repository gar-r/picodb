package picodb

import (
	"compress/gzip"
	"encoding/gob"
	"io"
)

func (p *PicoDb) encode(w io.Writer, data interface{}) error {
	var writer io.Writer
	if p.opt.Compression {
		gz := gzip.NewWriter(w)
		defer gz.Close()
		writer = gz
	} else {
		writer = w
	}
	e := gob.NewEncoder(writer)
	return e.Encode(data)
}

func (p *PicoDb) decode(r io.Reader, data interface{}) error {
	var reader io.Reader
	if p.opt.Compression {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = r
	}
	e := gob.NewDecoder(reader)
	return e.Decode(data)
}
