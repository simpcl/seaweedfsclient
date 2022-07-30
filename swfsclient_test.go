package swfsclient

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var sc *SwfsClient
var SmallFile string

func init() {
	masterURL := os.Getenv("SWFS_MASTER_URL")
	if masterURL == "" {
		panic("Master URL is required")
	}

	sc, _ = NewSwfsClient(masterURL, &http.Client{Timeout: 5 * time.Minute}, 1024*1024)

	SmallFile = os.Getenv("SWFS_SMALL_FILE")
}

func TestLookup(t *testing.T) {
	_, err := sc.Lookup("1", nil)
	require.Nil(t, err)
}

func TestGrowAndGC(t *testing.T) {
	err := sc.GC(1024 * 1024)
	require.Nil(t, err)
}

func TestStatus(t *testing.T) {
	_, err := sc.Status()
	require.Nil(t, err)
}

func TestClusterStatus(t *testing.T) {
	_, err := sc.ClusterStatus()
	require.Nil(t, err)
}

func TestDownloadFile(t *testing.T) {
	result, err := sc.Submit(SmallFile, "", "")
	require.Nil(t, err)
	require.NotNil(t, result)

	// return fake error
	_, err = sc.Download(result.FileID, nil, func(r io.Reader) error {
		return fmt.Errorf("Fake error")
	})
	require.NotNil(t, err)

	// verifying
	verifyDownloadFile(t, result.FileID)
}

func verifyDownloadFile(t *testing.T, fid string) (data []byte) {
	_, err := sc.Download(fid, nil, func(r io.Reader) (err error) {
		data, err = ioutil.ReadAll(r)
		return
	})
	require.Nil(t, err)
	require.NotZero(t, len(data))
	return
}
