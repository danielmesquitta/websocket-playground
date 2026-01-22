# WebSocket Playground

A real-time chat room application built with Go, demonstrating WebSocket communication using the Hub/Client pattern.

## Features

- Real-time messaging with WebSocket connections
- Hub pattern for efficient client management and message broadcasting
- Username-based identification
- System notifications for user join/leave events
- Modern, responsive UI with gradient styling
- Single-file application with embedded HTML/CSS/JS

## Architecture

The application uses a Hub/Client architecture common in WebSocket applications:

```
┌─────────────────────────────────────────────────────────┐
│                         Hub                             │
│  - Manages all connected clients                        │
│  - Broadcasts messages to all clients                   │
│  - Handles client registration/unregistration           │
└─────────────────────────────────────────────────────────┘
           │                    │                    │
           ▼                    ▼                    ▼
      ┌─────────┐          ┌─────────┐          ┌─────────┐
      │ Client  │          │ Client  │          │ Client  │
      │ (User1) │          │ (User2) │          │ (User3) │
      └─────────┘          └─────────┘          └─────────┘
```

### Components

- **Hub**: Central coordinator that maintains the set of active clients and broadcasts messages to all connected clients
- **Client**: Represents a single WebSocket connection with read/write pumps for bidirectional communication
- **Message**: JSON structure containing username, content, and timestamp

## Prerequisites

- Go 1.25 or higher

## Installation

1. Clone the repository:

```bash
git clone https://github.com/danielmesquitta/websocket-playground.git
cd websocket-playground
```

2. Install dependencies:

```bash
go mod download
```

## Running the Application

Start the server:

```bash
go run main.go
```

The server will start on `http://localhost:8080`.

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Serves the chat room HTML page |
| `/ws` | GET | WebSocket endpoint for real-time communication |

### WebSocket Connection

Connect to the WebSocket endpoint with an optional username parameter:

```
ws://localhost:8080/ws?username=YourName
```

If no username is provided, the user will be identified as "Anonymous".

### Message Format

Messages are sent and received as JSON:

```json
{
  "username": "John",
  "content": "Hello, everyone!",
  "created_at": "2024-01-15T10:30:00Z"
}
```

## Usage

1. Open your browser and navigate to `http://localhost:8080`
2. Enter your username in the input field
3. Click "Join Chat" to connect to the chat room
4. Type messages and press Enter or click "Send" to broadcast to all connected users
5. System messages will notify when users join or leave the chat

## Tech Stack

- [Gin](https://github.com/gin-gonic/gin) - HTTP web framework
- [Gorilla WebSocket](https://github.com/gorilla/websocket) - WebSocket implementation

## License

MIT
