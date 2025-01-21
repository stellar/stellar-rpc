package protocol

const GetVersionInfoMethodName = "getVersionInfo"

type GetVersionInfoResponse struct {
	Version            string `json:"version"`
	CommitHash         string `json:"commitHash"`
	BuildTimestamp     string `json:"buildTimestamp"`
	CaptiveCoreVersion string `json:"captiveCoreVersion"`
	ProtocolVersion    uint32 `json:"protocolVersion"`
	//nolint:tagliatelle
	CommitHashDeprecated string `json:"commit_hash"`
	//nolint:tagliatelle
	BuildTimestampDeprecated string `json:"build_time_stamp"`
	//nolint:tagliatelle
	CaptiveCoreVersionDeprecated string `json:"captive_core_version"`
	//nolint:tagliatelle
	ProtocolVersionDeprecated uint32 `json:"protocol_version"`
}
