# WhatsApp Business Backend

Node.js/Express backend powering a WhatsApp-style business messaging platform (Wati clone), handling real-time customer conversations, broadcasts, team management, and automation via the WhatsApp Cloud API.

## Tech Stack
- Node.js + Express
- PostgreSQL (via Supabase)
- Socket.IO (real-time messaging)
- WhatsApp Cloud API (Meta)
- Multer (media uploads)

## Features
- WhatsApp message send/receive (text, image, video, audio, document)
- Real-time team inbox with Socket.IO
- Contact management
- Broadcast messaging
- Team & tag assignment
- Automations: working hours, holiday mode, auto-replies (welcome, OOO)
- Webhook integration with Meta

## Environment Variables

Create a `.env` file in the root with:
ACCESS_TOKEN=your_whatsapp_access_token
PHONE_NUMBER_ID=your_phone_number_id
DB_CONNECTION_STRING=your_postgres_connection_string
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_anon_key

## Setup Instructions

1. Clone the repository
```bash
   git clone <repo-url>
   cd whatsapp-backend
```

2. Install dependencies
```bash
   npm install
```

3. Create `.env` file with the variables above

4. Run the server
```bash
   node server.js
```

Server runs on port 3000 by default.

## API Endpoints (Overview)
- `POST /api/login` / `POST /api/signup` — Authentication
- `GET /conversations` — Fetch inbox
- `POST /send-message` — Send WhatsApp message
- `POST /broadcast` — Send broadcast to eligible contacts
- `GET/POST /api/contacts` — Contact management
- `GET/POST /api/teams` — Team management
- `GET/POST /api/tags` — Tag management
- `GET/PATCH /api/settings/*` — Business automation settings
- `POST /webhook` — WhatsApp webhook receiver

## Project Status
🚧 In Development

## Author
Wajid — BAI-22F-019