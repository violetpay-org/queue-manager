package qmanutils

import (
	"path/filepath"
	"runtime"
	"strings"
)

func GetRootPath(rootPath string) string {
	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)

	// raise error if rootPath is not in the basepath
	if !strings.Contains(basepath, rootPath) {
		panic("rootPath is not in the basepath")
	}

	// trim the diractories until we get rootPath
	for !strings.HasSuffix(basepath, rootPath) {
		basepath = filepath.Dir(basepath)
	}

	return basepath
}
