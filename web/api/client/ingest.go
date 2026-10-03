package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/internal/metricstore"
	v1 "github.com/komari-monitor/komari/protocol/v1"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	agent_runtime "github.com/komari-monitor/komari/web/agent"
)

// ingest.go
// agent 上报数据的传输无关处理逻辑。v1 (REST/WS) 与 v2 (JSON-RPC) 两套上报入口
// 经过各自的协议解析后，统一调用这里的函数落库并更新运行时状态，消除重复。

// ingestReport 保存一次负载上报并刷新运行时状态。
// protocolVersion 标记上报所用协议（1 或 2），用于运行时区分客户端能力。
// markPresence 为 true 时按 POST 上报会话刷新在线状态（WS 连接自行管理在线状态，应传 false）。
func ingestReport(uuid string, report v2.Report, protocolVersion int, markPresence bool) error {
	report.UUID = uuid
	report.UpdatedAt = time.Now().UTC()
	if err := clients.ReportVerify(report); err != nil {
		return err
	}
	savedReport, err := metricstore.WriteReport(context.Background(), report)
	if err != nil {
		return err
	}
	agent_runtime.RecordReport(savedReport)
	agent_runtime.SetClientProtocolVersion(uuid, protocolVersion)
	if markPresence {
		refreshPostPresence(uuid, protocolVersion)
	}
	return nil
}

// ingestV1Report 供 v1 兼容入口使用：把 v1.Report 转为 v2.Report 后走统一流水线。
// v1/v2 的 Report JSON 结构一致，直接做一次 JSON 往返转换即可。
func ingestV1Report(uuid string, report v1.Report, markPresence bool) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	var converted v2.Report
	if err := json.Unmarshal(raw, &converted); err != nil {
		return err
	}
	return ingestReport(uuid, converted, 1, markPresence)
}

// ingestBasicInfo 保存客户端基础信息。fallbackIP 在上报未携带 IP 时用作兜底。
func ingestBasicInfo(uuid string, info map[string]interface{}, fallbackIP string) error {
	if info == nil {
		info = map[string]interface{}{}
	}
	return saveClientBasicInfo(info, uuid, fallbackIP)
}

// ingestPingResult 保存一条 ping 探测结果。
func ingestPingResult(uuid string, taskID uint, value int) error {
	return tasks.SavePingRecord(models.PingRecord{
		Client: uuid,
		TaskId: taskID,
		Value:  value,
		Time:   time.Now().UTC(),
	})
}
