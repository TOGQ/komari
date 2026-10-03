package terminal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	agent_runtime "github.com/komari-monitor/komari/web/agent"
	"github.com/komari-monitor/komari/web/connection"
)

// startFakeAgent 启动一个 WS 服务端，把服务端连接注册为 agent 连接，
// 返回客户端连接。读写方向：dispatchTerminalRequest 经服务端连接写，
// 测试从客户端连接读。
func startFakeAgent(t *testing.T, uuid string) *websocket.Conn {
	t.Helper()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		sc := connection.NewSafeConn(ws)
		agent_runtime.SetConnectedClients(uuid, sc)
		// 保持连接直到测试结束
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { agent_runtime.DeleteConnectedClients(uuid) })

	client, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial fake agent: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	deadline := time.Now().Add(5 * time.Second)
	for agent_runtime.GetConnectedClients()[uuid] == nil {
		if time.Now().After(deadline) {
			t.Fatal("fake agent connection not registered in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return client
}

func readOneMessage(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read agent message: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(msg, &m); err != nil {
		t.Fatalf("invalid agent message %q: %v", msg, err)
	}
	return m
}

func TestDispatchTerminalRequestV1Fallback(t *testing.T) {
	client := startFakeAgent(t, "v1-agent")

	if !dispatchTerminalRequest("v1-agent", "req-1") {
		t.Fatal("dispatchTerminalRequest returned false for connected v1 agent")
	}
	m := readOneMessage(t, client)
	if m["message"] != "terminal" || m["request_id"] != "req-1" {
		t.Fatalf("v1 agent got unexpected payload: %v", m)
	}
	if _, ok := m["jsonrpc"]; ok {
		t.Fatalf("v1 agent must not receive JSON-RPC payload: %v", m)
	}
}

func TestDispatchTerminalRequestV2(t *testing.T) {
	client := startFakeAgent(t, "v2-agent")
	agent_runtime.MarkV2Client("v2-agent")

	if !dispatchTerminalRequest("v2-agent", "req-2") {
		t.Fatal("dispatchTerminalRequest returned false for connected v2 agent")
	}
	m := readOneMessage(t, client)
	if m["jsonrpc"] != "2.0" || m["method"] != "agent.terminal.request" {
		t.Fatalf("v2 agent got unexpected payload: %v", m)
	}
	params, _ := m["params"].(map[string]any)
	if params["request_id"] != "req-2" {
		t.Fatalf("v2 agent got unexpected params: %v", m)
	}
}

func TestDispatchTerminalRequestOffline(t *testing.T) {
	if dispatchTerminalRequest("v1-offline-agent", "req-3") {
		t.Fatal("dispatchTerminalRequest should return false for offline v1 agent")
	}
}
