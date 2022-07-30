package swfsclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type SwfsClient struct {
	master      *url.URL
	client      *httpClient
	maxFileSize int64
}

func NewSwfsClient(masterURL string, client *http.Client, fileSizeLimit int64) (c *SwfsClient, err error) {
	u, err := parseURI(masterURL)
	if err != nil {
		return
	}

	c = &SwfsClient{
		master:      u,
		client:      newHTTPClient(client),
		maxFileSize: fileSizeLimit,
	}

	return
}

func (c *SwfsClient) Close() (err error) {
	if c.client != nil {
		err = c.client.Close()
	}
	return
}

// Grow pre-Allocate Volumes.
func (c *SwfsClient) Grow(count int, collection, replication, dataCenter string) error {
	args := normalize(nil, collection, "")
	if count > 0 {
		args.Set(ParamGrowCount, strconv.Itoa(count))
	}
	if replication != "" {
		args.Set(ParamGrowReplication, replication)
	}
	if dataCenter != "" {
		args.Set(ParamGrowDataCenter, dataCenter)
	}
	return c.GrowArgs(args)
}

// GrowArgs pre-Allocate volumes with args.
func (c *SwfsClient) GrowArgs(args url.Values) (err error) {
	_, _, err = c.client.get(encodeURI(*c.master, "/vol/grow", args), nil)
	return
}

func (c *SwfsClient) DeleteCollection(args url.Values) (err error) {
	_, _, err = c.client.get(encodeURI(*c.master, "/col/delete", args), nil)
	return
}

// Lookup volume ID.
func (c *SwfsClient) Lookup(volID string, args url.Values) (result *LookupResult, err error) {
	result, err = c.doLookup(volID, args)
	return
}

func (c *SwfsClient) doLookup(volID string, args url.Values) (result *LookupResult, err error) {
	args = normalize(args, "", "")
	args.Set(ParamLookupVolumeID, volID)

	jsonBlob, _, err := c.client.get(encodeURI(*c.master, "/dir/lookup", args), nil)
	if err == nil {
		result = &LookupResult{}
		if err = json.Unmarshal(jsonBlob, result); err == nil {
			if result.Error != "" {
				err = errors.New(result.Error)
			}
		}
	}

	return
}

// LookupServerByFileID lookup server by file id.
func (c *SwfsClient) LookupServerByFileID(fileID string, args url.Values, readonly bool) (server string, err error) {
	var parts []string
	if strings.Contains(fileID, ",") {
		parts = strings.Split(fileID, ",")
	} else {
		parts = strings.Split(fileID, "/")
	}

	if len(parts) != 2 { // wrong file id format
		return "", errors.New("Invalid fileID " + fileID)
	}

	lookup, lookupError := c.Lookup(parts[0], args)
	if lookupError != nil {
		err = lookupError
	} else if len(lookup.VolumeLocations) == 0 {
		err = ErrFileNotFound
	}

	if err == nil {
		if readonly {
			server = lookup.VolumeLocations.RandomPickForRead().PublicURL
		} else {
			server = lookup.VolumeLocations.Head().URL
		}
	}

	return
}

// LookupFileID lookup file by id.
func (c *SwfsClient) LookupFileID(fileID string, args url.Values, readonly bool) (fullURL string, err error) {
	u, err := c.LookupServerByFileID(fileID, args, readonly)
	if err == nil {
		base := *c.master
		base.Host = u
		base.Path = fileID
		fullURL = base.String()
	}
	return
}

// GC force Garbage Collection.
func (c *SwfsClient) GC(threshold float64) (err error) {
	args := url.Values{
		"garbageThreshold": []string{strconv.FormatFloat(threshold, 'f', -1, 64)},
	}
	_, _, err = c.client.get(encodeURI(*c.master, "/vol/vacuum", args), nil)
	return
}

// Status check System Status.
func (c *SwfsClient) Status() (result *SystemStatus, err error) {
	data, _, err := c.client.get(encodeURI(*c.master, "/dir/status", nil), nil)
	if err == nil {
		result = &SystemStatus{}
		err = json.Unmarshal(data, result)
	}
	return
}

// ClusterStatus get cluster status.
func (c *SwfsClient) ClusterStatus() (result *ClusterStatus, err error) {
	data, _, err := c.client.get(encodeURI(*c.master, "/cluster/status", nil), nil)
	if err == nil {
		result = &ClusterStatus{}
		err = json.Unmarshal(data, result)
	}
	return
}

// Assign do assign api.
func (c *SwfsClient) Assign(args url.Values) (result *AssignResult, err error) {
	jsonBlob, _, err := c.client.get(encodeURI(*c.master, "/dir/assign", args), nil)
	if err == nil {
		result = &AssignResult{}
		if err = json.Unmarshal(jsonBlob, result); err != nil {
			err = fmt.Errorf("/dir/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
		} else if result.Count == 0 {
			err = errors.New(result.Error)
		}
	}

	return
}

// Submit file directly to master.
func (c *SwfsClient) Submit(filePath string, collection string, ttl string) (result *SubmitResult, err error) {
	f, err := NewSwFile(filePath)
	if err != nil {
		return
	}
	defer f.Close()

	var data []byte
	args := normalize(nil, collection, ttl)
	data, _, err = c.client.upload(encodeURI(*c.master, "/submit", args), f.FileName, f.Reader, f.MimeType)
	if err != nil {
		return
	}

	result = &SubmitResult{}
	err = json.Unmarshal(data, result)
	return
}

func (c *SwfsClient) UploadSwFile(f *SwFile) (assignResult *AssignResult, err error) {
	// Assign first to get file id and url for uploading
	assignResult, err = c.Assign(normalize(nil, f.Collection, f.TTL))
	if err != nil {
		return
	}

	base := *c.master
	base.Host = assignResult.URL

	// do upload
	var v []byte
	v, _, err = c.client.upload(
		encodeURI(base, assignResult.FileID, nil),
		f.FileName, io.LimitReader(f.Reader, c.maxFileSize),
		"application/octet-stream")
	if err != nil {
		return
	}
	// parsing response data
	uploadResult := UploadResult{}
	if err = json.Unmarshal(v, &uploadResult); err != nil {
		return
	}
	if f.FileSize != uploadResult.Size {
		errors.New("wrong upload size")
	}

	return
}

// Download file by id.
func (c *SwfsClient) Download(fileID string, args url.Values, callback func(io.Reader) error) (fileName string, err error) {
	fileURL, err := c.LookupFileID(fileID, args, true)
	if err == nil {
		fileName, err = c.client.download(fileURL, callback)
	}
	return
}

// DeleteFile by id.
func (c *SwfsClient) DeleteFile(fileID string, args url.Values) (err error) {
	fileURL, err := c.LookupFileID(fileID, args, false)
	if err == nil {
		_, err = c.client.delete(fileURL)
	}
	return
}
