package xrayconf

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// xray 中需要合并（而非覆盖）的数组字段
var arrayFields = map[string]bool{
	"inbounds":  true,
	"outbounds": true,
}

// ReadConfig reads the xray config from configPath and/or confDir, merging as needed.
// When confDir is set, all *.json files are loaded in alphabetical order and merged.
func ReadConfig(configPath, confDir string) (map[string]interface{}, error) {
	var configs []map[string]interface{}

	// load single config file first (if provided)
	if configPath != "" {
		m, err := readJSONFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", configPath, err)
		}
		configs = append(configs, m)
	}

	// load confdir files in alphabetical order
	if confDir != "" {
		entries, err := os.ReadDir(confDir)
		if err != nil && configPath == "" {
			return nil, fmt.Errorf("read confdir %s: %w", confDir, err)
		}
		var jsonFiles []string
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if filepath.Ext(e.Name()) == ".json" {
				jsonFiles = append(jsonFiles, filepath.Join(confDir, e.Name()))
			}
		}
		sort.Strings(jsonFiles)
		for _, f := range jsonFiles {
			m, err := readJSONFile(f)
			if err != nil {
				continue
			}
			configs = append(configs, m)
		}
	}

	if len(configs) == 0 {
		return nil, fmt.Errorf("no xray config found")
	}

	return Merge(configs), nil
}

// Merge combines multiple xray config objects using xray's multi-config semantics:
//   - array fields (inbounds, outbounds): concatenate
//   - routing.rules: concatenate
//   - everything else: last wins
func Merge(configs []map[string]interface{}) map[string]interface{} {
	if len(configs) == 1 {
		return configs[0]
	}
	result := make(map[string]interface{})
	for _, cfg := range configs {
		for k, v := range cfg {
			if arrayFields[k] {
				existing, _ := result[k].([]interface{})
				if arr, ok := v.([]interface{}); ok {
					result[k] = append(existing, arr...)
				}
				continue
			}
			if k == "routing" {
				result[k] = mergeRouting(result[k], v)
				continue
			}
			result[k] = v
		}
	}
	return result
}

func mergeRouting(existing, incoming interface{}) interface{} {
	existMap, ok1 := existing.(map[string]interface{})
	incomMap, ok2 := incoming.(map[string]interface{})
	if !ok2 {
		return existing
	}
	if !ok1 {
		return incoming
	}
	// merge rules array
	existRules, _ := existMap["rules"].([]interface{})
	incomRules, _ := incomMap["rules"].([]interface{})
	for k, v := range incomMap {
		if k == "rules" {
			continue
		}
		existMap[k] = v
	}
	if len(incomRules) > 0 {
		existMap["rules"] = append(existRules, incomRules...)
	}
	return existMap
}

func readJSONFile(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// WriteAPIConfig writes the API-related config to confDir/99-mmwx-api.json
// (for confdir mode) or merges into the single config file.
func WriteAPIConfig(configPath, confDir string, sections map[string]interface{}) error {
	if confDir != "" {
		target := filepath.Join(confDir, "99-mmwx-api.json")
		data, _ := json.MarshalIndent(sections, "", "    ")
		return os.WriteFile(target, data, 0644)
	}

	// single file mode: merge into existing config
	content, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}

	// backup
	_ = os.WriteFile(configPath+".backup", content, 0644)

	var config map[string]interface{}
	if err := json.Unmarshal(content, &config); err != nil {
		return err
	}

	for k, v := range sections {
		if arrayFields[k] {
			existing, _ := config[k].([]interface{})
			if arr, ok := v.([]interface{}); ok {
				config[k] = append(existing, arr...)
			}
			continue
		}
		if k == "routing" {
			config[k] = mergeRouting(config[k], v)
			continue
		}
		config[k] = v
	}

	data, _ := json.MarshalIndent(config, "", "    ")
	return os.WriteFile(configPath, data, 0644)
}
