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
	"time"

	"github.com/patrickmn/go-cache"
)

type SwfsClient struct {
	master               *url.URL
	client               *httpClient
	maxFileSize          int64
	volumeLocationsCache *cache.Cache
}

func NewSwfsClient(masterURL string, client *http.Client, fileSizeLimit int64) (c *SwfsClient, err error) {
	u, err := parseURI(masterURL)
	if err != nil {
		return
	}

	c = &SwfsClient{
		master:               u,
		client:               newHTTPClient(client),
		maxFileSize:          fileSizeLimit,
		volumeLocationsCache: cache.New(5*time.Minute, 10*time.Minute),
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

func (c *SwfsClient) getVolumeIDFromFileID(fileID string) (string, error) {
	var parts []string
	if strings.Contains(fileID, ",") {
		parts = strings.Split(fileID, ",")
	} else {
		parts = strings.Split(fileID, "/")
	}

	if len(parts) != 2 { // wrong file id format
		return "", errors.New("Invalid fileID " + fileID)
	}

	return parts[0], nil
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
	var volumeID string
	volumeID, err = c.getVolumeIDFromFileID(fileID)
	if err != nil {
		return
	}

	lookup, lookupError := c.Lookup(volumeID, args)
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

func (c *SwfsClient) GetVolumeLocationsFromFileID(fileID string, args url.Values, withCache bool) (*VolumeLocations, error) {
	volumeID, err := c.getVolumeIDFromFileID(fileID)
	if err != nil {
		return nil, err
	}

	if withCache {
		if vls, found := c.volumeLocationsCache.Get(volumeID); found {
			return vls.(*VolumeLocations), nil
		}
	}

	lookupResult, lookupError := c.doLookup(volumeID, args)
	if lookupError != nil {
		return nil, lookupError
	} else if len(lookupResult.VolumeLocations) == 0 {
		return nil, ErrFileNotFound
	}
	c.volumeLocationsCache.Set(volumeID, &lookupResult.VolumeLocations, cache.DefaultExpiration)

	return &lookupResult.VolumeLocations, nil
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
func (c *SwfsClient) Submit(filePath string, collection, ttl string) (result *SubmitResult, err error) {
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

	args := normalize(nil, f.Collection, f.TTL)
	if f.ModTime != 0 {
		args.Set("ts", strconv.FormatInt(f.ModTime, 10))
	}

	f.FileID = assignResult.FileID
	if len(f.MimeType) == 0 {
		f.MimeType = "application/octet-stream"
	}

	// do upload
	var v []byte
	v, _, err = c.client.upload(
		encodeURI(base, f.FileID, args),
		f.FileName, io.LimitReader(f.Reader, c.maxFileSize),
		f.MimeType)
	if err != nil {
		return
	}

	// parsing response data
	uploadResult := UploadResult{}
	if err = json.Unmarshal(v, &uploadResult); err != nil {
		return
	}
	if f.FileSize != uploadResult.Size {
		err = errors.New("wrong upload size")
		return
	}
	f.Etag = uploadResult.Etag

	return
}

func (c *SwfsClient) UploadFile(filePath string, collection, ttl string) (assignResult *AssignResult, fp *SwFile, err error) {
	fp, err = NewSwFile(filePath)
	if err != nil {
		return
	}

	fp.Collection, fp.TTL = collection, ttl
	assignResult, err = c.UploadSwFile(fp)
	_ = fp.Close()
	return
}

// Download file by id.
func (c *SwfsClient) Download(fileID string, args url.Values, callback func(io.Reader) error) (string, error) {
	var withCache = true
	var err error = nil
	for retry := 2; retry > 0; retry-- {
		var vls *VolumeLocations = nil
		var fileName string
		vls, err = c.GetVolumeLocationsFromFileID(fileID, args, withCache)
		if err != nil {
			return "", err
		}

		fileURL := fmt.Sprintf("http://%s/%s", vls.RandomPickForRead().PublicURL, fileID)
		fileName, err = c.client.download(fileURL, callback)
		if err == nil {
			return fileName, nil
		}
		withCache = false
	}
	return "", err
}

// DeleteFile by id.
func (c *SwfsClient) DeleteFile(fileID string, args url.Values) error {
	var withCache = true
	var err error = nil
	for retry := 2; retry > 0; retry-- {
		var vls *VolumeLocations = nil
		vls, err = c.GetVolumeLocationsFromFileID(fileID, args, withCache)
		if err != nil {
			return err
		}

		fileURL := fmt.Sprintf("http://%s/%s", vls.Head().URL, fileID)
		_, err = c.client.delete(fileURL)
		if err == nil {
			return nil
		}
		withCache = false
	}
	return err
}
