package stepmanager

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"grit/manifest"
	"grit/types"
	"os"
	"path"
	"slices"
)

type ManagedStep struct {
	step types.Step
	dir  string
}

func NewManagedStep(dir string, mstep manifest.ManifestStep) ManagedStep {
	stepDir := path.Join(dir, step.Name)
	stepDirs, err := os.ReadDir(stepDir)
	if err != nil {
		panic(err)
	}

	latestVersion := len(stepDirs)

	latestDir := path.Join(stepDir, fmt.Sprintf("%d", latestVersion))

	currentScriptHash := sha256.Sum256([]byte(mstep.Script))

	hashpath := path.Join(latestDir, "hash")

	latestScriptHash, err := os.ReadFile(hashpath)
	if err != nil {
		panic(err)
	}

	d := latestDir
	v := latestVersion

	if slices.Compare(currentScriptHash[:], latestScriptHash) != len(currentScriptHash) {
		nextVersion := latestVersion + 1
		nextDir := path.Join(stepDir, fmt.Sprintf("%d", nextVersion))
		nextHash := path.Join(nextDir, "hash")

		os.MkdirAll(nextDir, os.ModeDir)

		f, err := os.Create(nextHash)
		if err != nil {
			panic(err)
		}

		_, err = f.Write(currentScriptHash[:])
		if err != nil {
			panic(err)
		}

		d = nextDir
		v = nextVersion
	}

	step := types.Step{
		Name:       mstep.Name,
		Script:     mstep.Script,
		ScriptHash: hex.EncodeToString(currentScriptHash[:]),
		Parallel:   mstep.Parallel,
		Input:      mstep.Input,
		Timeout:    mstep.Timeout,
		Version:    v,
	}

	return ManagedStep{step, d}
}

func (m ManagedStep) Dir() string {
	return m.dir
}

func (m ManagedStep) Step() types.Step {
	return m.step
}
