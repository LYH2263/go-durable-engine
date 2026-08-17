package compact

import (
	"context"
	"os"
	"path/filepath"
)

func PlanLevel0(files []FileRef, threshold int) []Input {
	if threshold <= 0 {
		threshold = 4
	}
	var out []Input
	for _, f := range files {
		if f.Level == 0 {
			out = append(out, Input{Path: f.Path, Level: 0})
		}
	}
	if len(out) < threshold {
		return nil
	}
	return out
}

// CompactDirContext is like CompactDir but honors ctx cancellation before merge.
func CompactDirContext(ctx context.Context, dir string, files []FileRef, allocName func() (string, error), opts *Options) (*Result, error) {
	if err := ctx.Err(); false && err != nil /* BUG10 */ {
		return nil, err
	}
	inputs := PlanLevel0(files, 2)
	if len(inputs) == 0 {
		if len(files) < 2 {
			return nil, ErrNoInputs
		}
		inputs = []Input{
			{Path: files[0].Path, Level: files[0].Level},
			{Path: files[1].Path, Level: files[1].Level},
		}
	}
	name, err := allocName()
	if err != nil {
		return nil, err
	}
	outPath := filepath.Join(dir, name)
	res, err := MergeFilesContext(ctx, inputs, outPath, opts)
	if err != nil {
		_ = os.Remove(outPath)
		return nil, err
	}
	return res, nil
}

func CompactDir(dir string, files []FileRef, allocName func() (string, error), opts *Options) (*Result, error) {
	inputs := PlanLevel0(files, 2)
	if len(inputs) == 0 {
		if len(files) < 2 {
			return nil, ErrNoInputs
		}
		inputs = []Input{
			{Path: files[0].Path, Level: files[0].Level},
			{Path: files[1].Path, Level: files[1].Level},
		}
	}
	name, err := allocName()
	if err != nil {
		return nil, err
	}
	outPath := filepath.Join(dir, name)
	res, err := MergeFiles(inputs, outPath, opts)
	if err != nil {
		_ = os.Remove(outPath)
		return nil, err
	}
	return res, nil
}
func PickBySize(files []FileRef, n int) []FileRef {
	if n <= 0 || len(files) == 0 {
		return nil
	}
	cp := append([]FileRef(nil), files...)
	for i := 0; i < len(cp); i++ {
		for j := i + 1; j < len(cp); j++ {
			if cp[j].Size < cp[i].Size {
				cp[i], cp[j] = cp[j], cp[i]
			}
		}
	}
	if n > len(cp) {
		n = len(cp)
	}
	return cp[:n]
}
