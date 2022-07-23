package swfsclient

import "fmt"

var (
	// ErrFileNotFound return file not found error
	ErrFileNotFound = fmt.Errorf("File not found")
)

const (
	// ParamCollection http param to specify collection which files belong. According to SeaweedFS API.
	ParamCollection = "collection"

	// ParamTTL http param to specify time to live. According to SeaweedFS API.
	ParamTTL = "ttl"

	// ParamCount http param to specify how many file ids to reserve. According to SeaweedFS API.
	ParamCount = "count"

	// ParamAssignReplication http param to assign files with a specific replication type.
	ParamAssignReplication = "replication"

	// ParamAssignCount http param to specify how many file ids to reserve.
	ParamAssignCount = "count"

	// ParamAssignDataCenter http param to assign a specific data center
	ParamAssignDataCenter = "dataCenter"

	// ParamLookupVolumeID http param to specify volume ID for looking up.
	ParamLookupVolumeID = "volumeId"

	// ParamLookupPretty http param to make json response prettified or not. Default should not be set.
	ParamLookupPretty = "pretty"

	// ParamLookupCollection http param to specify known collection, this would make file look up/search faster.
	ParamLookupCollection = "collection"

	// ParamVacuumGarbageThreshold if your system has many deletions, the deleted file's disk space will not be synchronously re-claimed.
	// There is a background job to check volume disk usage. If empty space is more than the threshold,
	// default to 0.3, the vacuum job will make the volume readonly, create a new volume with only existing files,
	// and switch on the new volume. If you are impatient or doing some testing, vacuum the unused spaces this way.
	ParamVacuumGarbageThreshold = "GarbageThreshold"

	// ParamGrowReplication http param to specify a specific replication.
	ParamGrowReplication = "replication"

	// ParamGrowCount http param to specify number of empty volume to grow.
	ParamGrowCount = "count"

	// ParamGrowDataCenter http param to specify datacenter of growing volume.
	ParamGrowDataCenter = "dataCenter"

	// ParamGrowCollection http param to specify collection of files for growing.
	ParamGrowCollection = "collection"

	// ParamGrowTTL specify time to live for growing api. Refers to: https://github.com/chrislusf/seaweedfs/wiki/Store-file-with-a-Time-To-Live
	// 3m: 3 minutes
	// 4h: 4 hours
	// 5d: 5 days
	// 6w: 6 weeks
	// 7M: 7 months
	// 8y: 8 years
	ParamGrowTTL = "ttl"

	// admin operations
	// ParamAssignVolumeReplication = "replication"
	// ParamAssignVolume            = "volume"
	// ParamDeleteVolume            = "volume"
	// ParamMountVolume             = "volume"
	// ParamUnmountVolume           = "volume"
)
