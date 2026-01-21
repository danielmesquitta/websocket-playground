package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// Message represents a chat message
type Message struct {
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// Client represents a single WebSocket connection
type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan Message
	username string
}

// Hub maintains the set of active clients and broadcasts messages
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan Message
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("Client %s connected. Total clients: %d", client.username, len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("Client %s disconnected. Total clients: %d", client.username, len(h.clients))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// readPump pumps messages from the WebSocket connection to the hub
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		msg := Message{
			Username:  c.username,
			Content:   string(message),
			CreatedAt: time.Now(),
		}
		c.hub.broadcast <- msg
	}
}

// writePump pumps messages from the hub to the WebSocket connection
func (c *Client) writePump() {
	defer func() {
		c.conn.Close()
	}()

	for message := range c.send {
		data, err := json.Marshal(message)
		if err != nil {
			log.Printf("error marshaling message: %v", err)
			continue
		}
		err = c.conn.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			return
		}
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

func serveWs(hub *Hub, c *gin.Context) {
	username := c.Query("username")
	if username == "" {
		username = "Anonymous"
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	client := &Client{
		hub:      hub,
		conn:     conn,
		send:     make(chan Message, 256),
		username: username,
	}

	hub.register <- client

	// Send welcome message
	welcomeMsg := Message{
		Username:  "System",
		Content:   "Welcome to the chat room, " + username + "!",
		CreatedAt: time.Now(),
	}
	client.send <- welcomeMsg

	// Notify others
	joinMsg := Message{
		Username:  "System",
		Content:   username + " has joined the chat",
		CreatedAt: time.Now(),
	}
	hub.broadcast <- joinMsg

	go client.writePump()
	client.readPump()

	// Notify others when leaving
	leaveMsg := Message{
		Username:  "System",
		Content:   username + " has left the chat",
		CreatedAt: time.Now(),
	}
	hub.broadcast <- leaveMsg
}

func main() {
	hub := newHub()
	go hub.run()

	r := gin.Default()

	// Serve the HTML page
	r.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html")
		c.String(http.StatusOK, indexHTML)
	})

	// WebSocket endpoint
	r.GET("/ws", func(c *gin.Context) {
		serveWs(hub, c)
	})

	log.Println("Server starting on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WebSocket Chat Room</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }

        .container {
            width: 100%;
            max-width: 600px;
        }

        .chat-container {
            background: white;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            overflow: hidden;
        }

        .chat-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            text-align: center;
        }

        .chat-header h1 {
            font-size: 1.5rem;
            font-weight: 600;
        }

        .status {
            font-size: 0.85rem;
            margin-top: 5px;
            opacity: 0.9;
        }

        .status-dot {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 6px;
        }

        .status-dot.connected {
            background: #4ade80;
        }

        .status-dot.disconnected {
            background: #f87171;
        }

        #login-form {
            padding: 30px;
        }

        #login-form input {
            width: 100%;
            padding: 14px 18px;
            border: 2px solid #e5e7eb;
            border-radius: 10px;
            font-size: 1rem;
            margin-bottom: 15px;
            transition: border-color 0.2s;
        }

        #login-form input:focus {
            outline: none;
            border-color: #667eea;
        }

        #login-form button {
            width: 100%;
            padding: 14px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 10px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s, box-shadow 0.2s;
        }

        #login-form button:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(102, 126, 234, 0.4);
        }

        #chat-area {
            display: none;
        }

        #messages {
            height: 400px;
            overflow-y: auto;
            padding: 20px;
            background: #f9fafb;
        }

        .message {
            margin-bottom: 12px;
            padding: 12px 16px;
            border-radius: 12px;
            max-width: 85%;
            word-wrap: break-word;
        }

        .message.system {
            background: #e5e7eb;
            color: #6b7280;
            font-style: italic;
            text-align: center;
            max-width: 100%;
            font-size: 0.9rem;
        }

        .message.user {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            margin-left: auto;
        }

        .message.other {
            background: white;
            border: 1px solid #e5e7eb;
        }

        .message .username {
            font-weight: 600;
            font-size: 0.85rem;
            margin-bottom: 4px;
            opacity: 0.8;
        }

        .message.user .username {
            color: rgba(255, 255, 255, 0.8);
        }

        .message.other .username {
            color: #667eea;
        }

        .input-area {
            display: flex;
            padding: 20px;
            background: white;
            border-top: 1px solid #e5e7eb;
            gap: 12px;
        }

        #message-input {
            flex: 1;
            padding: 14px 18px;
            border: 2px solid #e5e7eb;
            border-radius: 10px;
            font-size: 1rem;
            transition: border-color 0.2s;
        }

        #message-input:focus {
            outline: none;
            border-color: #667eea;
        }

        #send-btn {
            padding: 14px 28px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 10px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s, box-shadow 0.2s;
        }

        #send-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(102, 126, 234, 0.4);
        }

        #send-btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="chat-container">
            <div class="chat-header">
                <h1>💬 Chat Room</h1>
                <div class="status">
                    <span class="status-dot disconnected" id="status-dot"></span>
                    <span id="status-text">Disconnected</span>
                </div>
            </div>

            <div id="login-form">
                <input type="text" id="username-input" placeholder="Enter your username..." maxlength="20">
                <button onclick="connect()">Join Chat</button>
            </div>

            <div id="chat-area">
                <div id="messages"></div>
                <div class="input-area">
                    <input type="text" id="message-input" placeholder="Type a message..." maxlength="500">
                    <button id="send-btn" onclick="sendMessage()">Send</button>
                </div>
            </div>
        </div>
    </div>

    <script>
        let ws;
        let username;

        function connect() {
            username = document.getElementById('username-input').value.trim();
            if (!username) {
                alert('Please enter a username');
                return;
            }

            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(protocol + '//' + window.location.host + '/ws?username=' + encodeURIComponent(username));

            ws.onopen = function() {
                document.getElementById('status-dot').className = 'status-dot connected';
                document.getElementById('status-text').textContent = 'Connected as ' + username;
                document.getElementById('login-form').style.display = 'none';
                document.getElementById('chat-area').style.display = 'block';
                document.getElementById('message-input').focus();
            };

            ws.onclose = function() {
                document.getElementById('status-dot').className = 'status-dot disconnected';
                document.getElementById('status-text').textContent = 'Disconnected';
                document.getElementById('send-btn').disabled = true;
                addMessage({ username: 'System', content: 'Connection lost. Please refresh to reconnect.', created_at: new Date().toISOString() }, 'system');
            };

            ws.onerror = function(error) {
                console.error('WebSocket error:', error);
            };

            ws.onmessage = function(event) {
                const message = JSON.parse(event.data);
                let type = 'other';
                
                if (message.username === 'System') {
                    type = 'system';
                } else if (message.username === username) {
                    type = 'user';
                }
                
                addMessage(message, type);
            };
        }

        function formatTime(dateString) {
            const date = new Date(dateString);
            return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }

        function addMessage(message, type) {
            const messagesDiv = document.getElementById('messages');
            const messageEl = document.createElement('div');
            messageEl.className = 'message ' + type;

            if (type === 'system') {
                messageEl.textContent = message.content;
            } else {
                const usernameEl = document.createElement('div');
                usernameEl.className = 'username';
                usernameEl.textContent = message.username + ' · ' + formatTime(message.created_at);

                const contentEl = document.createElement('div');
                contentEl.textContent = message.content;

                messageEl.appendChild(usernameEl);
                messageEl.appendChild(contentEl);
            }

            messagesDiv.appendChild(messageEl);
            messagesDiv.scrollTop = messagesDiv.scrollHeight;
        }

        function sendMessage() {
            const input = document.getElementById('message-input');
            const message = input.value.trim();
            
            if (message && ws && ws.readyState === WebSocket.OPEN) {
                ws.send(message);
                input.value = '';
            }
        }

        document.getElementById('message-input').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                sendMessage();
            }
        });

        document.getElementById('username-input').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                connect();
            }
        });
    </script>
</body>
</html>`
