package swfsclient

import (
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type SwFile struct {
	Reader     io.ReadCloser
	FileName   string
	FileSize   int64
	MimeType   string
	ModTime    int64 //in seconds
	Collection string

	// TTL Time to live.
	// 3m: 3 minutes
	// 4h: 4 hours
	// 5d: 5 days
	// 6w: 6 weeks
	// 7M: 7 months
	// 8y: 8 years
	TTL string

	Server string
	FileID string
	Etag   string
}

// Close underlying openned file.
func (f *SwFile) Close() (err error) {
	err = f.Reader.Close()
	return
}

// NewSwFileFromReader from file reader.
// fileName and fileSize must be known
func NewSwFileFromReader(reader io.ReadCloser, fileName string, fileSize int64) *SwFile {
	ret := SwFile{
		Reader:   reader,
		FileSize: fileSize,
		FileName: fileName,
	}

	ext := strings.ToLower(path.Ext(fileName))
	if ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	return &ret
}

// NewSwFile from real file dir
func NewSwFile(fullPathFilename string) (*SwFile, error) {
	fh, openErr := os.Open(fullPathFilename)
	if openErr != nil {
		return nil, openErr
	}

	ret := SwFile{
		Reader:   fh,
		FileName: filepath.Base(fullPathFilename),
	}

	if fi, fiErr := fh.Stat(); fiErr == nil {
		ret.ModTime = fi.ModTime().UTC().Unix()
		ret.FileSize = fi.Size()
	} else {
		return nil, fiErr
	}

	ext := strings.ToLower(path.Ext(ret.FileName))
	if ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	return &ret, nil
}

// NewFileParts create many file part at once.
func NewSwFiles(fullPathFilenames []string) (fps []*SwFile, err error) {
	fps = make([]*SwFile, 0, len(fullPathFilenames))
	for _, file := range fullPathFilenames {
		if fp, err := NewSwFile(file); err == nil {
			fps = append(fps, fp)
		} else {
			closeSwFiles(fps)
			return nil, err
		}
	}
	return
}

func closeSwFiles(fps []*SwFile) {
	for i := range fps {
		_ = fps[i].Close()
	}
}
