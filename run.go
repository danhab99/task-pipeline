package main

import (
	"fmt"
	"os"
	"os/exec"
)

func runStep(s *Step, db Database) {
	fmt.Printf("Running step %d for task: %s\n", s.ID, s.Task.Name)
	cmd := exec.Command("sh", "-c", s.Task.Script.String)

	input_file, err := os.CreateTemp("/tmp", "*")
	if err != nil {
		panic(err)
	}
	output_dir := os.TempDir()

	input_file.Write(s.Object)

	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", input_file.Name()),
		fmt.Sprintf("OUTPUT_DIR=%s", output_dir),
	)

	fmt.Printf("  Executing script with INPUT_FILE=%s\n", input_file.Name())
	_, err = cmd.CombinedOutput()
	if err != nil {
		panic(err)
	}

	dirs, err := os.ReadDir(output_dir)
	if err != nil {
		panic(err)
	}

	outputCount := 0
	for _, file := range dirs {
		if file.IsDir() {
			continue
		}

		body, err := os.ReadFile(file.Name())
		if err != nil {
			panic(err)
		}

		err = db.InsertStep(Step{
			TaskID:   s.TaskID,
			Object:   body,
			PrevStep: s,
		})
		if err != nil {
			panic(err)
		}
		outputCount++
	}
	fmt.Printf("  Generated %d output files\n", outputCount)

}
