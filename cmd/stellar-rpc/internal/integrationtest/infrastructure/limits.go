package infrastructure

import (
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
)

const upgradesDir = "docker/upgrades"

// resolveLimitFile picks the upgrade file for the limits called name at the
// given protocol version. Files are named <name>.p<N>.xdr. The file with the
// highest N that is not greater than version is used, so a new file is only
// needed when the config XDR actually changes between protocols.
func resolveLimitFile(fsys fs.FS, name string, version int32) (string, error) {
	re := regexp.MustCompile(`^` + regexp.QuoteMeta(name) + `\.p(\d+)\.xdr$`)
	entries, err := fs.ReadDir(fsys, upgradesDir)
	if err != nil {
		return "", err
	}

	best, bestVersion := "", int64(-1)
	var candidates []string
	for _, entry := range entries {
		m := re.FindStringSubmatch(entry.Name())
		if m == nil {
			continue
		}
		candidates = append(candidates, entry.Name())
		v, err := strconv.ParseInt(m[1], 10, 32)
		if err != nil {
			return "", fmt.Errorf("invalid protocol version in %s: %w", entry.Name(), err)
		}
		if v <= int64(version) && v > bestVersion {
			best, bestVersion = entry.Name(), v
		}
	}
	if best == "" {
		return "", fmt.Errorf("no %s.p<N>.xdr file in %s with N <= %d (found: %v)",
			name, upgradesDir, version, candidates)
	}
	return best, nil
}
