package app

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/joho/godotenv"
)

const defaultEnvFile = "/workspace/.env"

// loadEnvironmentFile adds variables that are not already present in the
// process environment. A missing file is optional; any other read or parse
// error is returned so startup cannot silently use incomplete configuration.
func loadEnvironmentFile(path string) (bool, error) {
	if err := godotenv.Load(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("load environment file %q: %w", path, err)
	}
	return true, nil
}
