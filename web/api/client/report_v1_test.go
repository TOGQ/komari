package client

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	v1 "github.com/komari-monitor/komari/protocol/v1"
	v2 "github.com/komari-monitor/komari/protocol/v2"
)

func TestV1ToV2ReportConversion(t *testing.T) {
	in := v1.Report{
		UUID:    "abc-123",
		CPU:     v1.CPUReport{Name: "Intel", Cores: 8, Arch: "amd64", Usage: 12.5},
		Ram:     v1.RamReport{Total: 16000, Used: 8000},
		Swap:    v1.RamReport{Total: 4000, Used: 100},
		Load:    v1.LoadReport{Load1: 0.5, Load5: 0.4, Load15: 0.3},
		Disk:    v1.DiskReport{Total: 100000, Used: 40000},
		Network: v1.NetworkReport{Up: 100, Down: 200, TotalUp: 1000, TotalDown: 2000},
		Connections: v1.ConnectionsReport{TCP: 10, UDP: 5},
		GPU: &v1.GPUDetailReport{Count: 1, AverageUsage: 33.3, DetailedInfo: []v1.GPUDeviceInfo{
			{Name: "RTX", MemoryTotal: 8000, MemoryUsed: 2000, Utilization: 33.3, Temperature: 60},
		}},
		Uptime:    9999,
		Process:   123,
		Message:   "hello",
		Method:    "ws",
		UpdatedAt: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC),
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out v2.Report
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	var m1, m2 map[string]any
	json.Unmarshal(raw, &m1)
	raw2, _ := json.Marshal(out)
	json.Unmarshal(raw2, &m2)
	if !reflect.DeepEqual(m1, m2) {
		t.Fatalf("conversion lossy:\n%s\n%s", raw, raw2)
	}
	if out.UUID != "abc-123" || out.CPU.Usage != 12.5 || out.GPU.DetailedInfo[0].Temperature != 60 {
		t.Fatal("field values mismatch")
	}
}
