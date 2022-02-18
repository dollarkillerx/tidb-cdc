package tidb_cdc

import (
	"strings"
	"time"
)

// GetColumnNameFromTag ...
func GetColumnNameFromTag(tag string) string {
	// 按分号分割
	tags := strings.Split(tag, ";")
	for _, value := range tags {
		v := strings.Split(value, ":")
		k := strings.TrimSpace(strings.ToUpper(v[0]))
		if len(v) >= 2 {
			if k == "COLUMN" {
				return strings.Join(v[1:], ":")
			}
		}
	}
	return ""
}

// isTimestampType ...
func isTimestampType(gTag string) bool {
	typ := getColumnTypeFromTag(gTag)
	if typ == "TIMESTAMP" {
		return true
	}
	return false
}

func getColumnTypeFromTag(tag string) string {
	// 按分号分割
	tags := strings.Split(tag, ";")
	for _, value := range tags {
		v := strings.Split(value, ":")
		k := strings.TrimSpace(strings.ToUpper(v[0]))
		if len(v) >= 2 && k == "TYPE" {
			return strings.TrimSpace(strings.ToUpper(v[1]))
		}
	}
	return ""
}

// formatTime ...
func formatTime(date string) (*time.Time, error) {
	layout := "2006-01-02 15:04:05"

	t, err := time.ParseInLocation(layout, date, time.FixedZone("BJ", 8*3600))
	if err != nil {
		return nil, err
	}
	return &t, nil
}
