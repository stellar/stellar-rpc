package bench

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// profileFlags is the optional Go-profiling flag pair shared by both
// subcommands.
type profileFlags struct {
	cpuProfile   string
	memProfile   string
	memProfileAt int
}

func (p *profileFlags) bind(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringVar(&p.cpuProfile, "cpuprofile", "", "write a Go CPU profile to PATH")
	fs.StringVar(&p.memProfile, "memprofile", "",
		"write a Go allocation profile to PATH (read it with `go tool pprof -alloc_space`)")
	fs.IntVar(&p.memProfileAt, "memprofile-at", 0,
		"ALSO write a live-heap snapshot (read with -inuse_space) to PATH.at after this many seconds — "+
			"for locating a mid-run peak the exit profile cannot see (requires --memprofile)")
}

// around runs fn under the optional profiles: the CPU profile spans the whole
// run; the heap profile is written once fn returns (see writeHeapProfile).
func (p *profileFlags) around(logger *supportlog.Entry, fn func() error) error {
	if p.cpuProfile != "" {
		f, err := os.Create(p.cpuProfile)
		if err != nil {
			return fmt.Errorf("create %s: %w", p.cpuProfile, err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return fmt.Errorf("start CPU profile: %w", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
			logger.Infof("wrote CPU profile to %s", p.cpuProfile)
		}()
	}
	if p.memProfileAt > 0 && p.memProfile != "" {
		go func() {
			time.Sleep(time.Duration(p.memProfileAt) * time.Second)
			path := p.memProfile + ".at"
			f, err := os.Create(path)
			if err != nil {
				logger.Warnf("memprofile-at: create %s: %v", path, err)
				return
			}
			defer f.Close()
			runtime.GC() // settle live-set so inuse_space is the truth, not pacing slack
			if err := pprof.WriteHeapProfile(f); err != nil {
				logger.Warnf("memprofile-at: write %s: %v", path, err)
				return
			}
			logger.Infof("wrote mid-run heap snapshot to %s", path)
		}()
	}
	err := fn()
	if p.memProfile != "" {
		p.writeHeapProfile(logger)
	}
	return err
}

// writeHeapProfile writes the heap profile to --memprofile. Read it as an
// ALLOCATION profile (`go tool pprof -alloc_space`): that view is cumulative —
// every byte the run allocated, whether or not it was later freed — so writing
// it here, after the run's stores have closed, still captures the full picture.
// The default `-inuse_space` view is a live snapshot taken after teardown and
// under-reports, so it is not what this profile is for. No GC is forced:
// alloc_space is unaffected by collection, so there is nothing to gain by
// perturbing the run.
func (p *profileFlags) writeHeapProfile(logger *supportlog.Entry) {
	f, err := os.Create(p.memProfile)
	if err != nil {
		logger.Errorf("create %s: %v", p.memProfile, err)
		return
	}
	defer func() { _ = f.Close() }()
	if err := pprof.WriteHeapProfile(f); err != nil {
		logger.Errorf("write heap profile: %v", err)
		return
	}
	logger.Infof("wrote heap profile to %s", p.memProfile)
}
