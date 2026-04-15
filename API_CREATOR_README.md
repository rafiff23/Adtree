# Creator Registry API

Simple REST API to fetch creator data from `public.creator_registry` table.

## Setup

### Installation
```bash
pip install -r requirements_api.txt
```

### Environment Variables
```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=adtree
export PG_USER=postgres
export PG_PASSWORD=4dtr33
export API_KEY=your-secret-api-key
```

### Running
```bash
python api_creator.py
```

The API will run on `http://0.0.0.0:8000`

---

## Endpoints

### 1. Get All Creators
**GET** `/api/creators`

**Headers:**
```
X-API-Key: your-secret-api-key
```

**Query Parameters:**
- `agency_id` (int, optional) - Filter by agency
- `binding_status` (string, optional) - "Bound" or "Unbound"
- `limit` (int, default: 1000, max: 10000) - Records per page
- `offset` (int, default: 0) - Pagination offset

**Example:**
```bash
curl -H "X-API-Key: your-secret-api-key" \
  "http://localhost:8000/api/creators?agency_id=1&limit=100&offset=0"
```

**Response:**
```json
{
  "success": true,
  "total": 450,
  "limit": 100,
  "offset": 0,
  "count": 100,
  "data": [
    {
      "id": 1,
      "tiktok_id": "creator_username",
      "followers": 10000,
      "full_name": "Creator Name",
      "domicile": "Jakarta",
      "binding_status": "Bound",
      "level": 1,
      "agency_id": 1,
      ...
    }
  ]
}
```

---

### 2. Get Creators by Agency
**GET** `/api/creators/by-agency/<agency_id>`

**Headers:**
```
X-API-Key: your-secret-api-key
```

**Query Parameters:**
- `limit` (int, default: 1000, max: 10000)
- `offset` (int, default: 0)

**Example:**
```bash
curl -H "X-API-Key: your-secret-api-key" \
  "http://localhost:8000/api/creators/by-agency/1"
```

---

### 3. Health Check
**GET** `/api/health`

No authentication required.

**Response:**
```json
{
  "status": "ok",
  "service": "Creator Registry API"
}
```

---

### 4. API Documentation
**GET** `/api/docs`

No authentication required. Returns full API documentation.

---

## Authentication

All data endpoints require the `X-API-Key` header:

```bash
curl -H "X-API-Key: your-secret-api-key" "http://localhost:8000/api/creators"
```

---

## Deployment on VPS

### 1. Set up a systemd service (or use supervisor)

Create `/etc/systemd/system/creator-api.service`:

```ini
[Unit]
Description=Creator Registry API
After=network.target

[Service]
Type=simple
User=adtree
WorkingDirectory=/home/adtree/apps/creator_api
Environment="PATH=/home/adtree/apps/creator_api/venv/bin"
Environment="PG_HOST=localhost"
Environment="PG_PORT=5432"
Environment="PG_DB=adtree"
Environment="PG_USER=postgres"
Environment="PG_PASSWORD=4dtr33"
Environment="API_KEY=your-secret-key-here"
ExecStart=/home/adtree/apps/creator_api/venv/bin/python api_creator.py
Restart=always

[Install]
WantedBy=multi-user.target
```

### 2. Start the service

```bash
sudo systemctl daemon-reload
sudo systemctl start creator-api
sudo systemctl enable creator-api
```

### 3. Proxy with nginx (optional)

```nginx
server {
    listen 80;
    server_name api.adtreedigital.cloud;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

---

## Response Codes

- `200` - Success
- `400` - Bad request
- `401` - Unauthorized (invalid/missing API key)
- `500` - Server error

---

## Rate Limiting

Currently not enforced, but can be added with `flask-limiter`.

---

## Security Notes

⚠️ **Change the API_KEY in production!**

- Never commit real API keys
- Use environment variables
- Consider using JWT tokens for more secure authentication
- Add IP whitelisting if needed
