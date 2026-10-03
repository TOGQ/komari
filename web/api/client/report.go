package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	logger "github.com/komari-monitor/komari/utils/log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/database/clients"
	v1 "github.com/komari-monitor/komari/protocol/v1"
	"github.com/komari-monitor/komari/utils/notifier"
	agent_runtime "github.com/komari-monitor/komari/web/agent"
	"github.com/komari-monitor/komari/web/api"
	"github.com/komari-monitor/komari/web/connection"
)

// report.go
// v1 兼容上报入口（REST/WS），供旧版 agent 使用。
// 解析为 v1.Report 后经 ingestV1Report 转为 v2 结构走统一落库流水线，
// 协议版本标记为 1，用于 ping/exec 下发的 v1 回退逻辑。

func UploadReport(c *gin.Context) {
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		logger.ErrorArgs("client-api", "Failed to read request body:", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	var data map[string]interface{}
	err = json.Unmarshal(bodyBytes, &data)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	// Save report to database
	var report v1.Report
	err = json.Unmarshal(bodyBytes, &report)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}
	// 优先使用 body 中的 UUID，若为空则从中间件注入的上下文中获取
	uuid := report.UUID
	if uuid == "" {
		if v, ok := c.Get("client_uuid"); ok {
			uuid, _ = v.(string)
		}
	}
	if uuid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "UUID is required"})
		return
	}

	// POST 上报：落库、更新运行时状态并刷新在线状态
	if err := ingestV1Report(uuid, report, true); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("%v", err)})
		return
	}

	c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // Restore the body for further use
	c.JSON(200, gin.H{"status": "success"})
}

func WebSocketReport(c *gin.Context) {
	// 升级ws
	if !api.IsWebSocketUpgrade(c) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "Require WebSocket upgrade"})
		return
	}
	// Upgrade the HTTP connection to a WebSocket connection
	conn, err := api.UpgradeSafeConn(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "Failed to upgrade to WebSocket." + err.Error()})
		return
	}
	defer conn.Close()

	_, message, err := conn.ReadMessage()
	if err != nil {
		logger.ErrorArgs("client-api", "Error reading message:", err)
		return
	}

	// 第一次数据拿token
	data := map[string]interface{}{}
	err = json.Unmarshal(message, &data)
	if err != nil {
		conn.WriteJSON(gin.H{"status": "error", "error": "Invalid JSON"})
		return
	}
	// it should ok,token was verfied in the middleware
	token := ""
	var errMsg string

	// 优先检查查询参数中的 token
	token = c.Query("token")

	// 如果 token 为空，返回错误
	if token == "" {
		conn.WriteJSON(gin.H{"status": "error", "error": errMsg})
		return
	}

	uuid, err := clients.GetClientUUIDByToken(token)
	if err != nil {
		conn.WriteJSON(gin.H{"status": "error", "error": errMsg})
		return
	}

	// 接受新连接，并处理旧连接
	if oldConn, exists := agent_runtime.GetConnectedClients()[uuid]; exists {
		logger.Infof("client-api", "Client %s is reconnecting. Closing the old connection.", uuid)

		// 强制关闭旧连接。这将导致旧连接的 ReadMessage() 循环出错退出。
		go oldConn.Close()
	}
	agent_runtime.SetConnectedClients(uuid, conn)
	agent_runtime.SetClientProtocolVersion(uuid, 1)
	logger.Infof("client-api", "Client %s is reconnect success, connID: %d", uuid, conn.ID)
	go notifier.OnlineNotification(uuid, conn.ID)
	defer func() {
		agent_runtime.DeleteClientConditionally(uuid, conn)
		notifier.OfflineNotification(uuid, conn.ID)
	}()

	// 首先处理第一次ws conn收到的消息
	processMessage(conn, message, uuid)

	for {
		conn.SetReadDeadline(time.Now().Add(readWait))

		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Errorf("client-api", "Client %s connection error: %v", uuid, err)
			}
			break // 任何读错误（包括超时）都意味着连接已断开，退出循环
		}
		processMessage(conn, message, uuid)
	}
}

// 将消息处理逻辑提取到一个函数中，方便复用
func processMessage(conn *connection.SafeConn, message []byte, uuid string) {
	type MessageType struct {
		Type string `json:"type"`
	}
	var msgType MessageType
	err := json.Unmarshal(message, &msgType)
	if err != nil {
		conn.WriteJSON(gin.H{"status": "error", "error": "Invalid JSON"})
		return
	}

	switch msgType.Type {
	case "", "report":
		report := v1.Report{}
		err = json.Unmarshal(message, &report)
		if err != nil {
			conn.WriteJSON(gin.H{"status": "error", "error": "Invalid report format"})
			return
		}
		// WS 连接自行管理在线状态，无需刷新 POST presence
		if err := ingestV1Report(uuid, report, false); err != nil {
			conn.WriteJSON(gin.H{"status": "error", "error": fmt.Sprintf("%v", err)})
			return
		}
	case "ping_result":
		var reqBody struct {
			PingTaskID uint   `json:"task_id"`
			PingResult int    `json:"value"`
			PingType   string `json:"ping_type"`
		}
		err = json.Unmarshal(message, &reqBody)
		if err != nil {
			conn.WriteJSON(gin.H{"status": "error", "error": "Invalid ping result format"})
			return
		}
		if err := ingestPingResult(uuid, reqBody.PingTaskID, reqBody.PingResult); err != nil {
			conn.WriteJSON(gin.H{"status": "error", "error": "Failed to save ping result"})
		}
	default:
		logger.Warnf("client-api", "Unknown message type: %s", msgType.Type)
		conn.WriteJSON(gin.H{"status": "error", "error": "Unknown message type"})
	}
}
