require('dotenv').config()
const express = require("express")
const bodyParser = require("body-parser")
const { Pool } = require("pg")
const multer = require("multer")
const path = require("path")
const fs = require("fs")
const axios = require("axios")
const FormData = require("form-data")
const http = require("http")
const { Server } = require("socket.io")
const cors = require("cors")
const { createClient } = require('@supabase/supabase-js')
const ws = require('ws')
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_KEY,
    { realtime: { transport: ws } }
)

const app = express()
const server = http.createServer(app)
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
})

app.use(cors())
app.use(bodyParser.json())
app.use(express.static("public"))
app.use("/uploads", express.static("uploads"))

const ACCESS_TOKEN = process.env.ACCESS_TOKEN
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID

if (!fs.existsSync("uploads")) fs.mkdirSync("uploads")

const upload = multer({
    storage: multer.diskStorage({
        destination: (req, file, cb) => cb(null, "uploads/"),
        filename: (req, file, cb) => cb(null, Date.now() + "-" + file.originalname)
    })
})

const pool = new Pool({
    connectionString: process.env.DB_CONNECTION_STRING,
    ssl: { rejectUnauthorized: false }
})

/* ================= DB INIT ================= */
// Ensures all required tables exist on startup
async function initDB() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            phone_number VARCHAR(30) UNIQUE NOT NULL,
            name VARCHAR(100),
            avatar_url TEXT,
            allow_broadcast BOOLEAN DEFAULT true,
            allow_sms BOOLEAN DEFAULT true,
            user_id INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS conversations (
            id SERIAL PRIMARY KEY,
            contact_id INTEGER REFERENCES contacts(id),
            last_message TEXT,
            last_message_time TIMESTAMP DEFAULT NOW(),
            status VARCHAR(20) DEFAULT 'Open',
            assigned_to VARCHAR(100),
            unread_count INTEGER DEFAULT 0,
            user_id INTEGER,
            team_id INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            conversation_id INTEGER REFERENCES conversations(id),
            sender VARCHAR(20),
            message_text TEXT,
            type VARCHAR(20) DEFAULT 'text',
            media_url TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE NOT NULL,
            password VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS broadcasts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            message TEXT NOT NULL,
            total_contacts INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            skipped_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS tags (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) UNIQUE NOT NULL,
            color VARCHAR(20) DEFAULT '#006d2f',
            user_id INTEGER REFERENCES users(id),
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS conversation_tags (
            conversation_id INTEGER REFERENCES conversations(id) ON DELETE CASCADE,
            tag_id INTEGER REFERENCES tags(id) ON DELETE CASCADE,
            PRIMARY KEY (conversation_id, tag_id)
        );

        CREATE TABLE IF NOT EXISTS teams (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL,
            description TEXT,
            user_id INTEGER REFERENCES users(id),
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS business_settings (
            user_id INTEGER PRIMARY KEY REFERENCES users(id),
            timezone VARCHAR(50) DEFAULT 'GMT+05:00',
            holiday_mode_on BOOLEAN DEFAULT false,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS working_hours (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            day VARCHAR(10) NOT NULL,
            is_open BOOLEAN DEFAULT true,
            open_time VARCHAR(10) DEFAULT '09:00',
            close_time VARCHAR(10) DEFAULT '18:00',
            UNIQUE(user_id, day)
        );

        CREATE TABLE IF NOT EXISTS auto_replies (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            type VARCHAR(30) NOT NULL,
            enabled BOOLEAN DEFAULT true,
            reply_type VARCHAR(20) DEFAULT 'text',
            message_text TEXT,
            media_url TEXT,
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(user_id, type)
        );
    `)

    // 🆕 Auto-migration: ensures older/pre-existing tables also get the newer columns
    // (safe to run every time — IF NOT EXISTS prevents errors on repeated startup)
    await pool.query(`
        ALTER TABLE tags ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);
        ALTER TABLE teams ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);
        ALTER TABLE messages ADD COLUMN IF NOT EXISTS wa_message_id TEXT;
        ALTER TABLE messages ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'sent';
        ALTER TABLE messages ADD COLUMN IF NOT EXISTS deleted_for_everyone BOOLEAN DEFAULT false;
        ALTER TABLE messages ADD COLUMN IF NOT EXISTS pinned BOOLEAN DEFAULT false;
        ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to_id INTEGER REFERENCES messages(id);
    `)

    console.log("✅ DB tables ready")
}

initDB().catch(console.error)

/* ================= SOCKET ================= */

let onlineUsers = 0

io.on("connection", socket => {
    onlineUsers++
    io.emit("status", "Online")

    socket.on("typing", () => socket.broadcast.emit("typing", "Typing..."))
    socket.on("stop_typing", () => socket.broadcast.emit("typing", ""))

    socket.on("disconnect", () => {
        onlineUsers--
        if (onlineUsers <= 0) io.emit("status", "Offline")
    })
})

/* ================= HELPER: SAVE MESSAGE ================= */

async function saveMessage(phone, text, type, mediaUrl, sender, userId = null, waMessageId = null, replyToId = null) {
    // Upsert contact
    let c = await pool.query("SELECT id FROM contacts WHERE phone_number=$1", [phone])
    let contactId
    if (c.rows.length) {
        contactId = c.rows[0].id
    } else {
        const newContact = await pool.query(
            "INSERT INTO contacts(phone_number, name, user_id) VALUES($1, $2, $3) RETURNING id",
            [phone, phone, userId]
        )
        contactId = newContact.rows[0].id
    }

    // Upsert conversation
    let conv = await pool.query("SELECT id, unread_count FROM conversations WHERE contact_id=$1", [contactId])
    let convId
    if (conv.rows.length) {
        convId = conv.rows[0].id
        const newUnread = sender === "user" ? (conv.rows[0].unread_count || 0) + 1 : 0
        await pool.query(
            "UPDATE conversations SET last_message=$1, last_message_time=NOW(), unread_count=$2 WHERE id=$3",
            [text, newUnread, convId]
        )
    } else {
        const newConv = await pool.query(
            "INSERT INTO conversations(contact_id, last_message, last_message_time, unread_count) VALUES($1,$2,NOW(),$3) RETURNING id",
            [contactId, text, sender === "user" ? 1 : 0]
        )
        convId = newConv.rows[0].id
    }

    // Insert message
    const msg = await pool.query(
        `INSERT INTO messages(conversation_id, sender, message_text, type, media_url, wa_message_id, reply_to_id)
         VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
        [convId, sender, text, type, mediaUrl, waMessageId, replyToId]
    )

    // Fetch full conversation data to emit to inbox
    const fullConv = await pool.query(`
        SELECT
            conversations.id,
            contacts.phone_number,
            contacts.name,
            contacts.avatar_url,
            conversations.last_message,
            conversations.last_message_time,
            conversations.status,
            conversations.assigned_to,
            conversations.unread_count
        FROM conversations
        JOIN contacts ON contacts.id = conversations.contact_id
        WHERE conversations.id = $1
    `, [convId])

    // Emit new message to chat screen
    io.emit("new_message", {
        ...msg.rows[0],
        conversation_id: convId
    })

    // Emit conversation update to inbox (so new chats appear live)
    if (fullConv.rows.length) {
        io.emit("conversation_updated", fullConv.rows[0])
    }

    return { msgId: msg.rows[0].id, convId }
}

/* ================= HELPER: DOWNLOAD MEDIA ================= */

async function downloadMedia(mediaId, type = "", originalName = "") {
    try {
        const meta = await axios.get(`https://graph.facebook.com/v19.0/${mediaId}`, {
            headers: { Authorization: `Bearer ${ACCESS_TOKEN}` }
        })

        const response = await axios.get(meta.data.url, {
            responseType: "stream",
            headers: { Authorization: `Bearer ${ACCESS_TOKEN}` }
        })

        const ext = originalName?.split(".").pop() || "bin"
        const fileName = Date.now() + "." + ext
        const filePath = path.join(__dirname, "uploads", fileName)

        const writer = fs.createWriteStream(filePath)
        response.data.pipe(writer)

        return new Promise(resolve => {
            writer.on("finish", () => resolve({ url: "/uploads/" + fileName, name: originalName || fileName }))
            writer.on("error", () => resolve(null))
        })
    } catch (err) {
        console.error("downloadMedia error:", err.message)
        return null
    }
}

/* ================= HELPER: SEND TEXT ================= */

async function sendText(phone, message) {

    try {

        const response = await axios.post(
            `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
            {
                messaging_product: "whatsapp",
                recipient_type: "individual",
                to: phone,
                type: "text",
                text: {
                    preview_url: false,
                    body: message
                }
            },
            {
                headers: {
                    Authorization: `Bearer ${ACCESS_TOKEN}`,
                    "Content-Type": "application/json"
                }
            }
        )

        console.log("✅ WhatsApp message sent:", response.data)

        // 🆕 Return WhatsApp's message ID so we can track delivered/read status later
        return response.data.messages?.[0]?.id || null

    } catch (err) {

        console.log(
            "❌ WhatsApp Send Error:",
            err.response?.data || err.message
        )

        throw err
    }
}

/* ================= HELPER: SEND MEDIA ================= */

async function sendMedia(phone, filePath, type) {
    const form = new FormData()
    form.append("file", fs.createReadStream(filePath))
    form.append("messaging_product", "whatsapp")

    const uploadRes = await axios.post(
        `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/media`,
        form,
        { headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, ...form.getHeaders() } }
    )

    const mediaId = uploadRes.data.id
    let sendRes

    if (type === "audio") {
        sendRes = await axios.post(
            `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
            {
                messaging_product: "whatsapp",
                to: phone,
                type: "audio",
                audio: { id: mediaId, voice: true }
            },
            { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } }
        )
    } else {
        sendRes = await axios.post(
            `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
            {
                messaging_product: "whatsapp",
                to: phone,
                type,
                [type]: { id: mediaId }
            },
            { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } }
        )
    }

    // 🆕 Return WhatsApp's message ID so we can track delivered/read status later
    return sendRes.data.messages?.[0]?.id || null
}

/* ================= HELPER: BUSINESS TIMEZONE ================= */

function getBusinessNow(timezoneString) {
    const match = /GMT([+-])(\d{2}):(\d{2})/.exec(timezoneString || "GMT+00:00")
    let offsetMinutes = 0
    if (match) {
        const sign = match[1] === "+" ? 1 : -1
        offsetMinutes = sign * (parseInt(match[2]) * 60 + parseInt(match[3]))
    }
    const utcNow = new Date()
    return new Date(utcNow.getTime() + offsetMinutes * 60000)
}

/* ================= HELPER: AUTO-REPLY ENGINE ================= */

async function maybeSendAutoReply(phone, userId) {
    try {
        if (!userId) return

        // 1) Holiday mode check — highest priority
        const settingsRes = await pool.query(
            "SELECT * FROM business_settings WHERE user_id=$1",
            [userId]
        )
        const settings = settingsRes.rows[0]

        if (settings && settings.holiday_mode_on) {
            return await fireReply(userId, "advanced_ooo", phone)
        }

        // 2) Working hours check — business ke apne configured timezone (Timezone screen) ke hisab se
        const businessNow = getBusinessNow(settings && settings.timezone)
        const dayName = businessNow.toLocaleDateString("en-US", { weekday: "long", timeZone: "UTC" })
        const whRes = await pool.query(
            "SELECT * FROM working_hours WHERE user_id=$1 AND day=$2",
            [userId, dayName]
        )
        const todayHours = whRes.rows[0]

        if (todayHours && !todayHours.is_open) {
            return await fireReply(userId, "working_hours", phone)
        }

        if (todayHours && todayHours.is_open && todayHours.open_time && todayHours.close_time) {
            const nowMinutes = businessNow.getUTCHours() * 60 + businessNow.getUTCMinutes()
            const [oh, om] = todayHours.open_time.split(":").map(Number)
            const [ch, cm] = todayHours.close_time.split(":").map(Number)
            const openMinutes = oh * 60 + om
            const closeMinutes = ch * 60 + cm

            if (nowMinutes < openMinutes || nowMinutes > closeMinutes) {
                return await fireReply(userId, "working_hours", phone)
            }
        }

        // 3) New conversation? send welcome
        const contactRes = await pool.query("SELECT id FROM contacts WHERE phone_number=$1", [phone])
        if (contactRes.rows.length) {
            const convRes = await pool.query(
                "SELECT id, created_at FROM conversations WHERE contact_id=$1",
                [contactRes.rows[0].id]
            )
            if (convRes.rows.length) {
                const msgCountRes = await pool.query(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id=$1",
                    [convRes.rows[0].id]
                )
                // Agar ye is conversation ka pehla incoming message hai → welcome bhejo
                if (parseInt(msgCountRes.rows[0].count) <= 1) {
                    return await fireReply(userId, "welcome", phone)
                }
            }
        }
    } catch (err) {
        console.error("autoReply engine error:", err.message)
    }
}

async function fireReply(userId, type, phone) {
    const r = await pool.query(
        "SELECT * FROM auto_replies WHERE user_id=$1 AND type=$2",
        [userId, type]
    )
    const reply = r.rows[0]
    if (!reply || !reply.enabled || !reply.message_text) return

    const waId = await sendText(phone, reply.message_text)
    await saveMessage(phone, reply.message_text, "text", null, "agent", userId, waId)
}

/* ================= WEBHOOK VERIFY ================= */

app.get("/webhook", (req, res) => {
    const VERIFY_TOKEN = "wati_verify_token"
    const mode = req.query["hub.mode"]
    const token = req.query["hub.verify_token"]
    const challenge = req.query["hub.challenge"]

    if (mode === "subscribe" && token === VERIFY_TOKEN) {
        res.status(200).send(challenge)
    } else {
        res.sendStatus(403)
    }
})

/* ================= WEBHOOK RECEIVE ================= */

app.post("/webhook", async (req, res) => {
    try {
        const value = req.body.entry?.[0]?.changes?.[0]?.value

        // 🆕 Handle delivered/read status updates from WhatsApp
        const statuses = value?.statuses
        if (statuses && statuses.length) {
            for (const s of statuses) {
                try {
                    const updated = await pool.query(
                        "UPDATE messages SET status=$1 WHERE wa_message_id=$2 RETURNING id, conversation_id",
                        [s.status, s.id]
                    )
                    if (updated.rows.length) {
                        io.emit("message_status_update", {
                            message_id: updated.rows[0].id,
                            wa_message_id: s.id,
                            status: s.status,
                            conversation_id: updated.rows[0].conversation_id
                        })
                    }
                } catch (statusErr) {
                    console.error("status update error:", statusErr.message)
                }
            }
        }

        const msg = value?.messages?.[0]

        if (msg) {
            const phone = msg.from
            const type = msg.type
            let text = ""
            let mediaUrl = null

            if (type === "text") {
                text = msg.text.body
            } else if (type === "image") {
                const f = await downloadMedia(msg.image.id)
                text = msg.image.caption || "Image"
                mediaUrl = f?.url
            } else if (type === "audio") {
                const f = await downloadMedia(msg.audio.id)
                text = "Voice message"
                mediaUrl = f?.url
            } else if (type === "video") {
                const f = await downloadMedia(msg.video.id)
                text = msg.video.caption || "Video"
                mediaUrl = f?.url
            } else if (type === "document") {
                const f = await downloadMedia(msg.document.id, "", msg.document.filename)
                text = f?.name || "Document"
                mediaUrl = f?.url
            } else {
                text = type
            }

            const contactRes = await pool.query("SELECT user_id FROM contacts WHERE phone_number=$1", [phone])
            const ownerUserId = contactRes.rows[0]?.user_id || null

            await saveMessage(phone, text, type, mediaUrl, "user", ownerUserId, msg.id || null)
            await maybeSendAutoReply(phone, ownerUserId)   // 👈 NEW LINE
        }
    } catch (err) {
        console.error("Webhook error:", err.message)
    }

    res.sendStatus(200)
})

/* ================= SEND MESSAGE ================= */

app.post("/send-message", upload.single("media"), async (req, res) => {
    try {
        const { phone, message, reply_to_id } = req.body

        if (!phone) return res.status(400).json({ error: "phone is required" })

        if (message && message.trim()) {
            const waId = await sendText(phone, message)
            const userId = req.body.user_id || null
            await saveMessage(phone, message, "text", null, "agent", userId, waId, reply_to_id || null)
        }

        if (req.file) {
            const filePath = path.join(__dirname, req.file.path)
            const url = "/uploads/" + req.file.filename

            let type = "document"
            if (req.file.mimetype.includes("image")) type = "image"
            if (req.file.mimetype.includes("video")) type = "video"
            if (req.file.mimetype.includes("audio")) type = "audio"

            const waId = await sendMedia(phone, filePath, type)
            const userId = req.body.user_id || null
            await saveMessage(phone, req.file.originalname, type, url, "agent", userId, waId, reply_to_id || null)
        }

        res.json({ ok: true })
    } catch (err) {
        console.error("send-message error:", err.response?.data || err.message)
        res.status(500).json({ error: err.message })
    }
})

/* ================= GET CONVERSATIONS (INBOX) ================= */

app.get("/conversations", async (req, res) => {
    try {
        const userId = req.query.user_id
        let query = `
            SELECT
                conversations.id,
                contacts.phone_number,
                COALESCE(contacts.name, contacts.phone_number) AS name,
                contacts.avatar_url AS img,
                conversations.last_message AS msg,
                conversations.last_message_time,
                conversations.status,
                conversations.assigned_to AS assignee,
                conversations.team_id,
                COALESCE(teams.name, '') AS team_name,
                conversations.unread_count,
                COALESCE(
                    JSON_AGG(
                        JSON_BUILD_OBJECT('id', tags.id, 'name', tags.name, 'color', tags.color)
                    ) FILTER (WHERE tags.id IS NOT NULL),
                    '[]'
                ) AS tags
            FROM conversations
            JOIN contacts ON contacts.id = conversations.contact_id
            LEFT JOIN teams ON teams.id = conversations.team_id
            LEFT JOIN conversation_tags ON conversation_tags.conversation_id = conversations.id
            LEFT JOIN tags ON tags.id = conversation_tags.tag_id
            WHERE 1=1
        `
        const params = []
        if (userId) {
            params.push(userId)
            query += ` AND conversations.user_id = $${params.length}`
        }
        query += ` GROUP BY conversations.id, contacts.phone_number, contacts.name, contacts.avatar_url, conversations.last_message, conversations.last_message_time, conversations.status, conversations.assigned_to, conversations.team_id, teams.name, conversations.unread_count`
        query += ` ORDER BY conversations.last_message_time DESC`

        const r = await pool.query(query, params)
        res.json(r.rows)
    } catch (err) {
        console.error("GET /conversations error:", err.message)
        res.status(500).json({ error: err.message })
    }
})

/* ================= GET MESSAGES FOR A CONVERSATION ================= */

app.get("/conversations/:id/messages", async (req, res) => {
    try {
        const r = await pool.query(
            `SELECT
                id,
                conversation_id,
                sender,
                message_text AS text,
                type,
                media_url,
                status,
                pinned,
                deleted_for_everyone,
                reply_to_id,
                created_at
             FROM messages
             WHERE conversation_id=$1
             ORDER BY created_at ASC`,
            [req.params.id]
        )
        await pool.query(
            "UPDATE conversations SET unread_count=0 WHERE id=$1",
            [req.params.id]
        )
        res.json(r.rows)
    } catch (err) {
        console.error("GET messages error:", err.message)
        res.status(500).json({ error: err.message })
    }
})

/* ================= GET CONVERSATION DETAILS (for chat header) ================= */

app.get("/conversations/:id", async (req, res) => {
    try {
        const r = await pool.query(`
            SELECT
                conversations.id,
                contacts.id AS contact_id,
                contacts.phone_number,
                COALESCE(contacts.name, contacts.phone_number) AS name,
                contacts.avatar_url,
                conversations.status,
                conversations.assigned_to
            FROM conversations
            JOIN contacts ON contacts.id = conversations.contact_id
            WHERE conversations.id=$1
        `, [req.params.id])

        if (!r.rows.length) return res.status(404).json({ error: "Not found" })
        res.json(r.rows[0])
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

/* ================= UPDATE CONVERSATION STATUS ================= */

app.patch("/conversations/:id/status", async (req, res) => {
    try {
        const { status } = req.body
        await pool.query("UPDATE conversations SET status=$1 WHERE id=$2", [status, req.params.id])
        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

/* ================= MARK CONVERSATION AS READ (sends read receipt to customer) ================= */

app.post("/conversations/:id/mark-read", async (req, res) => {
    try {
        const lastMsg = await pool.query(
            "SELECT wa_message_id FROM messages WHERE conversation_id=$1 AND sender='user' AND wa_message_id IS NOT NULL ORDER BY created_at DESC LIMIT 1",
            [req.params.id]
        )
        if (lastMsg.rows.length && lastMsg.rows[0].wa_message_id) {
            await axios.post(
                `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
                { messaging_product: "whatsapp", status: "read", message_id: lastMsg.rows[0].wa_message_id },
                { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } }
            )
        }
        res.json({ ok: true })
    } catch (err) {
        console.error("mark-read error:", err.response?.data || err.message)
        res.json({ ok: false })
    }
})

/* ================= DELETE MESSAGE (for me / for everyone) ================= */

app.delete("/messages/:id", async (req, res) => {
    try {
        const { mode } = req.query // "me" or "everyone"
        if (mode === "everyone") {
            await pool.query(
                "UPDATE messages SET deleted_for_everyone=true, message_text='This message was deleted' WHERE id=$1",
                [req.params.id]
            )
        } else {
            await pool.query("DELETE FROM messages WHERE id=$1", [req.params.id])
        }
        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

/* ================= PIN / UNPIN MESSAGE ================= */

app.patch("/messages/:id/pin", async (req, res) => {
    try {
        const { pinned } = req.body
        await pool.query("UPDATE messages SET pinned=$1 WHERE id=$2", [pinned, req.params.id])
        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

/* ================= CONTACTS API ================= */

// Get all contacts
app.get("/api/contacts", async (req, res) => {
    try {
        const userId = req.query.user_id
        let query = `
            SELECT
                id,
                phone_number AS phone,
                COALESCE(name, phone_number) AS name,
                avatar_url,
                allow_broadcast,
                allow_sms,
                created_at
            FROM contacts
            WHERE 1=1
        `
        const params = []
        if (userId) {
            params.push(userId)
            query += ` AND user_id = $${params.length}`
        }
        query += ` ORDER BY name ASC`

        const r = await pool.query(query, params)
        res.json(r.rows)
    } catch (err) {
        console.error("GET /api/contacts error:", err.message)
        res.status(500).json({ error: err.message })
    }
})

// Get single contact
app.get("/api/contacts/:id", async (req, res) => {
    try {
        const r = await pool.query("SELECT * FROM contacts WHERE id=$1", [req.params.id])
        if (!r.rows.length) return res.status(404).json({ error: "Not found" })
        res.json(r.rows[0])
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Create contact
app.post("/api/contacts", async (req, res) => {
    try {
        const { name, phone, allow_broadcast = true, allow_sms = true, user_id = null } = req.body
        if (!phone) return res.status(400).json({ error: "phone is required" })

        const r = await pool.query(
            `INSERT INTO contacts(phone_number, name, allow_broadcast, allow_sms, user_id)
             VALUES($1,$2,$3,$4,$5)
             RETURNING *`,
            [phone, name || phone, allow_broadcast, allow_sms, user_id]
        )
        res.json(r.rows[0])
    } catch (err) {
        console.error("POST /api/contacts error:", err.message)
        res.status(500).json({ error: err.message })
    }
})

// Update contact
app.patch("/api/contacts/:id", async (req, res) => {
    try {
        const { name, allow_broadcast, allow_sms } = req.body
        await pool.query(
            "UPDATE contacts SET name=COALESCE($1,name), allow_broadcast=COALESCE($2,allow_broadcast), allow_sms=COALESCE($3,allow_sms) WHERE id=$4",
            [name, allow_broadcast, allow_sms, req.params.id]
        )
        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Delete contact
app.delete("/api/contacts/:id", async (req, res) => {
    try {
        await pool.query("DELETE FROM messages WHERE conversation_id IN (SELECT id FROM conversations WHERE contact_id=$1)", [req.params.id])
        await pool.query("DELETE FROM conversations WHERE contact_id=$1", [req.params.id])
        await pool.query("DELETE FROM contacts WHERE id=$1", [req.params.id])
        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

/* ================= AUTH API ================= */

// Login
app.post("/api/login", async (req, res) => {
    try {
        const { email, password } = req.body
        if (!email || !password) return res.status(400).json({ error: "email and password required" })

        const { data, error } = await supabase.auth.signInWithPassword({
            email,
            password
        })

        if (error) {
            if (error.message.includes('Email not confirmed')) {
                return res.status(401).json({ error: "Please confirm your email before logging in. Check your inbox or spam folder" })
            }
            return res.status(401).json({ error: "Invalid email or password" })
        }

        // Users table mein check karo
        const userRow = await pool.query("SELECT * FROM users WHERE email=$1", [email])

        if (!userRow.rows.length) {
            const newUser = await pool.query(
                "INSERT INTO users(name, email, password) VALUES($1,$2,$3) RETURNING *",
                [data.user.user_metadata?.name || email, email, password]
            )
            return res.json({
                id: newUser.rows[0].id,
                name: newUser.rows[0].name,
                email: newUser.rows[0].email,
                token: data.session.access_token
            })
        }

        res.json({
            id: userRow.rows[0].id,
            name: userRow.rows[0].name,
            email: userRow.rows[0].email,
            token: data.session.access_token
        })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Signup
app.post("/api/signup", async (req, res) => {
    try {
        const { name, email, password } = req.body
        if (!email) return res.status(400).json({ error: "Please enter your email." })
        if (!password || password.length < 6) return res.status(400).json({ error: "Password must be at least 6 characters." })

        const { data, error } = await supabase.auth.signUp({
            email,
            password,
            options: {
                data: { name: name || email }
            }
        })

        if (error) {
            if (error.message.includes('already registered')) {
                return res.status(400).json({ error: "An account with this email already exists. Please login instead." })
            }
            return res.status(400).json({ error: "Signup failed. Please try again." })
        }

        res.json({
            message: "success",
            email: email
        })
    } catch (err) {
        res.status(500).json({ error: "Server error. Please try again." })
    }
})
app.get("/debug-send", async (req, res) => {
    try {
        const response = await axios.post(
            `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
            {
                messaging_product: "whatsapp",
                recipient_type: "individual",
                to: "923302097907", // apna real number yahan
                type: "text",
                text: { preview_url: false, body: "Debug test" }
            },
            {
                headers: {
                    Authorization: `Bearer ${ACCESS_TOKEN}`,
                    "Content-Type": "application/json"
                }
            }
        )
        res.json({ success: true, data: response.data })
    } catch (err) {
        res.json({ success: false, error: err.response?.data || err.message })
    }
})
app.get("/test-whatsapp", async (req, res) => {

    try {

        const response = await axios.post(
            `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
            {
                messaging_product: "whatsapp",
                to: "923302097907",
                type: "text",
                text: {
                    body: "Test Message"
                }
            },
            {
                headers: {
                    Authorization: `Bearer ${ACCESS_TOKEN}`,
                    "Content-Type": "application/json"
                }
            }
        )

        res.json(response.data)

    } catch (err) {

        res.json(err.response?.data || err.message)
    }
})
app.get("/register-number", async (req, res) => {
    try {
        const response = await axios.post(
            `https://graph.facebook.com/v19.0/992179790645502/register`,
            {
                messaging_product: "whatsapp",
                pin: "123456"
            },
            {
                headers: {
                    Authorization: `Bearer ${ACCESS_TOKEN}`,
                    "Content-Type": "application/json"
                }
            }
        )
        res.json({ success: true, data: response.data })
    } catch (err) {
        res.json({ success: false, error: err.response?.data || err.message })
    }
})

app.post("/start-conversation", async (req, res) => {
    try {
        const { phone, user_id } = req.body
        if (!phone) return res.status(400).json({ error: "phone required" })

        // Template message bhejo
        const response = await axios.post(
            `https://graph.facebook.com/v19.0/${PHONE_NUMBER_ID}/messages`,
            {
                messaging_product: "whatsapp",
                to: phone,
                type: "template",
                template: {
                    name: "hello_world",
                    language: { code: "en_US" }
                }
            },
            {
                headers: {
                    Authorization: `Bearer ${ACCESS_TOKEN}`,
                    "Content-Type": "application/json"
                }
            }
        )

        // Conversation save karo
        await saveMessage(phone, "Hello! 👋", "text", null, "agent", user_id, response.data.messages?.[0]?.id || null)

        res.json({ ok: true, data: response.data })
    } catch (err) {
        console.error("start-conversation error:", err.response?.data || err.message)
        res.status(500).json({ error: err.response?.data?.error?.message || err.message })
    }
})

/* ================= BROADCAST MESSAGE ================= */

app.post("/broadcast", async (req, res) => {
    try {
        const { message, user_id } = req.body
        if (!message || !message.trim()) return res.status(400).json({ error: "message required" })
        if (!user_id) return res.status(400).json({ error: "user_id required" })

        // Sirf un contacts ko nikalo jinka allow_broadcast = true hai, isi user ke
        const contactsRes = await pool.query(
            "SELECT id, phone_number, name FROM contacts WHERE user_id=$1 AND allow_broadcast=true",
            [user_id]
        )

        const eligible = contactsRes.rows
        const results = { sent: [], skipped: [], failed: [] }

        for (const contact of eligible) {
            try {
                const waId = await sendText(contact.phone_number, message)
                await saveMessage(contact.phone_number, message, "text", null, "agent", user_id, waId)
                results.sent.push(contact.phone_number)
            } catch (err) {
                results.failed.push({ phone: contact.phone_number, error: err.response?.data?.error?.message || err.message })
            }
        }

        // Total contacts vs eligible — kitne skip huay (allow_broadcast=false)
        const totalRes = await pool.query(
            "SELECT COUNT(*) FROM contacts WHERE user_id=$1",
            [user_id]
        )
        const totalContacts = parseInt(totalRes.rows[0].count)
        const skippedCount = totalContacts - eligible.length
        await pool.query(
            `INSERT INTO broadcasts(user_id, message, total_contacts, sent_count, skipped_count, failed_count)
                VALUES($1,$2,$3,$4,$5,$6)`,
            [user_id, message, totalContacts, results.sent.length, skippedCount, results.failed.length]
        )
        res.json({
            ok: true,
            total_contacts: totalContacts,
            eligible_count: eligible.length,
            skipped_count: skippedCount,
            sent_count: results.sent.length,
            failed_count: results.failed.length,
            details: results
        })

    } catch (err) {
        console.error("broadcast error:", err.message)
        res.status(500).json({ error: err.message })
    }
})

app.get("/broadcasts", async (req, res) => {
    try {
        const userId = req.query.user_id
        const r = await pool.query(
            `SELECT id, message, total_contacts, sent_count, skipped_count, failed_count, created_at
             FROM broadcasts
             WHERE user_id=$1
             ORDER BY created_at DESC
             LIMIT 20`,
            [userId]
        )
        res.json(r.rows)
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})
/* ================= TEAMS API ================= */

// Get all teams
app.get("/api/teams", async (req, res) => {
    try {
        const r = await pool.query(
            "SELECT * FROM teams ORDER BY name ASC"
        )
        res.json(r.rows)
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Create team
app.post("/api/teams", async (req, res) => {
    try {
        const { name, description, user_id } = req.body
        if (!name) return res.status(400).json({ error: "Team name is required" })

        const r = await pool.query(
            `INSERT INTO teams(name, description, user_id)
             VALUES($1,$2,$3) RETURNING *`,
            [name, description || "", user_id || null]
        )
        res.json(r.rows[0])
    } catch (err) {
        if (err.code === "23505") {
            return res.status(409).json({ error: "Team name already exists" })
        }
        res.status(500).json({ error: err.message })
    }
})

// Delete team
app.delete("/api/teams/:id", async (req, res) => {
    try {
        // Pehle conversations se team unlink karo
        await pool.query(
            "UPDATE conversations SET team_id=NULL WHERE team_id=$1",
            [req.params.id]
        )
        await pool.query("DELETE FROM teams WHERE id=$1", [req.params.id])
        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Assign conversation to team
app.patch("/conversations/:id/team", async (req, res) => {
    try {
        const { team_id } = req.body
        await pool.query(
            "UPDATE conversations SET team_id=$1 WHERE id=$2",
            [team_id, req.params.id]
        )

        // Real-time update emit karo
        const conv = await pool.query(`
            SELECT
                conversations.id,
                contacts.phone_number,
                contacts.name,
                contacts.avatar_url,
                conversations.last_message,
                conversations.last_message_time,
                conversations.status,
                conversations.assigned_to,
                conversations.team_id,
                conversations.unread_count,
                teams.name AS team_name
            FROM conversations
            JOIN contacts ON contacts.id = conversations.contact_id
            LEFT JOIN teams ON teams.id = conversations.team_id
            WHERE conversations.id=$1
        `, [req.params.id])

        if (conv.rows.length) {
            io.emit("conversation_updated", conv.rows[0])
        }

        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Get all tags
app.get("/api/tags", async (req, res) => {
    try {
        const r = await pool.query("SELECT * FROM tags ORDER BY name ASC")
        res.json(r.rows)
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Create tag
app.post("/api/tags", async (req, res) => {
    try {
        const { name, color, user_id } = req.body
        if (!name) return res.status(400).json({ error: "name required" })
        const r = await pool.query(
            "INSERT INTO tags(name, color, user_id) VALUES($1,$2,$3) RETURNING *",
            [name.trim(), color || '#006d2f', user_id || null]
        )
        res.json(r.rows[0])
    } catch (err) {
        if (err.code === "23505") return res.status(409).json({ error: "Tag already exists" })
        res.status(500).json({ error: err.message })
    }
})

// Delete tag
app.delete("/api/tags/:id", async (req, res) => {
    try {
        await pool.query("DELETE FROM tags WHERE id=$1", [req.params.id])
        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Get tags for a conversation
app.get("/conversations/:id/tags", async (req, res) => {
    try {
        const r = await pool.query(
            `SELECT tags.* FROM tags
             JOIN conversation_tags ON tags.id = conversation_tags.tag_id
             WHERE conversation_tags.conversation_id = $1`,
            [req.params.id]
        )
        res.json(r.rows)
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

// Toggle tag on conversation (add if not exists, remove if exists)
app.post("/conversations/:id/tags", async (req, res) => {
    try {
        const { tag_id } = req.body
        const convId = req.params.id
        const exists = await pool.query(
            "SELECT 1 FROM conversation_tags WHERE conversation_id=$1 AND tag_id=$2",
            [convId, tag_id]
        )
        if (exists.rows.length) {
            await pool.query(
                "DELETE FROM conversation_tags WHERE conversation_id=$1 AND tag_id=$2",
                [convId, tag_id]
            )
            res.json({ action: "removed" })
        } else {
            await pool.query(
                "INSERT INTO conversation_tags(conversation_id, tag_id) VALUES($1,$2)",
                [convId, tag_id]
            )
            res.json({ action: "added" })
        }
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

/* ================= BUSINESS SETTINGS (Timezone + Holiday Mode) ================= */

app.get("/api/settings/business", async (req, res) => {
    try {
        const userId = req.query.user_id
        if (!userId) return res.status(400).json({ error: "user_id required" })

        let r = await pool.query("SELECT * FROM business_settings WHERE user_id=$1", [userId])
        if (!r.rows.length) {
            r = await pool.query(
                "INSERT INTO business_settings(user_id) VALUES($1) RETURNING *",
                [userId]
            )
        }
        res.json(r.rows[0])
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

app.patch("/api/settings/business", async (req, res) => {
    try {
        const { user_id, timezone, holiday_mode_on } = req.body
        if (!user_id) return res.status(400).json({ error: "user_id required" })

        const r = await pool.query(
            `INSERT INTO business_settings(user_id, timezone, holiday_mode_on)
             VALUES($1,$2,$3)
             ON CONFLICT (user_id) DO UPDATE SET
                timezone = COALESCE($2, business_settings.timezone),
                holiday_mode_on = COALESCE($3, business_settings.holiday_mode_on),
                updated_at = NOW()
             RETURNING *`,
            [user_id, timezone, holiday_mode_on]
        )
        res.json(r.rows[0])
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

/* ================= WORKING HOURS ================= */

const DEFAULT_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

app.get("/api/settings/working-hours", async (req, res) => {
    try {
        const userId = req.query.user_id
        if (!userId) return res.status(400).json({ error: "user_id required" })

        let r = await pool.query(
            "SELECT day, is_open, open_time, close_time FROM working_hours WHERE user_id=$1 ORDER BY id ASC",
            [userId]
        )

        if (!r.rows.length) {
            // First time — seed default rows (Mon-Fri open, Sat-Sun off)
            for (const day of DEFAULT_DAYS) {
                const isOpen = !["Saturday", "Sunday"].includes(day)
                await pool.query(
                    "INSERT INTO working_hours(user_id, day, is_open) VALUES($1,$2,$3)",
                    [userId, day, isOpen]
                )
            }
            r = await pool.query(
                "SELECT day, is_open, open_time, close_time FROM working_hours WHERE user_id=$1 ORDER BY id ASC",
                [userId]
            )
        }

        res.json(r.rows)
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

app.patch("/api/settings/working-hours", async (req, res) => {
    try {
        const { user_id, schedule } = req.body
        if (!user_id || !Array.isArray(schedule)) {
            return res.status(400).json({ error: "user_id and schedule[] required" })
        }

        for (const item of schedule) {
            await pool.query(
                `INSERT INTO working_hours(user_id, day, is_open, open_time, close_time)
                 VALUES($1,$2,$3,$4,$5)
                 ON CONFLICT (user_id, day) DO UPDATE SET
                    is_open = $3,
                    open_time = COALESCE($4, working_hours.open_time),
                    close_time = COALESCE($5, working_hours.close_time)`,
                [user_id, item.day, item.is_open, item.open_time || null, item.close_time || null]
            )
        }

        res.json({ ok: true })
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

/* ================= AUTO REPLIES ================= */

const VALID_REPLY_TYPES = ["ooo", "welcome", "working_hours", "advanced_ooo", "expired_chat"]

app.get("/api/auto-replies/:type", async (req, res) => {
    try {
        const { type } = req.params
        const userId = req.query.user_id
        if (!VALID_REPLY_TYPES.includes(type)) return res.status(400).json({ error: "invalid type" })
        if (!userId) return res.status(400).json({ error: "user_id required" })

        let r = await pool.query(
            "SELECT * FROM auto_replies WHERE user_id=$1 AND type=$2",
            [userId, type]
        )

        if (!r.rows.length) {
            r = await pool.query(
                `INSERT INTO auto_replies(user_id, type, enabled, reply_type, message_text)
                 VALUES($1,$2,true,'text','')
                 RETURNING *`,
                [userId, type]
            )
        }

        res.json(r.rows[0])
    } catch (err) {
        res.status(500).json({ error: err.message })
    }
})

app.post("/api/auto-replies/:type", upload.single("media"), async (req, res) => {
    try {
        const { type } = req.params
        if (!VALID_REPLY_TYPES.includes(type)) return res.status(400).json({ error: "invalid type" })

        const { user_id, enabled, reply_type, message_text } = req.body
        if (!user_id) return res.status(400).json({ error: "user_id required" })

        let mediaUrl = null
        if (req.file) {
            mediaUrl = "/uploads/" + req.file.filename
        }

        const r = await pool.query(
            `INSERT INTO auto_replies(user_id, type, enabled, reply_type, message_text, media_url)
             VALUES($1,$2,$3,$4,$5,$6)
             ON CONFLICT (user_id, type) DO UPDATE SET
                enabled = $3,
                reply_type = $4,
                message_text = $5,
                media_url = COALESCE($6, auto_replies.media_url),
                updated_at = NOW()
             RETURNING *`,
            [user_id, type, enabled === "true" || enabled === true, reply_type || "text", message_text || "", mediaUrl]
        )

        res.json(r.rows[0])
    } catch (err) {
        console.error("auto-reply save error:", err.message)
        res.status(500).json({ error: err.message })
    }
})

// ✅ SPA Catch-all — har route pe index.html serve karo
app.get(/.*/, (req, res) => {
    res.sendFile(path.join(__dirname, "public", "index.html"));
});
/* ================= START ================= */

const PORT = process.env.PORT || 3000
server.listen(PORT, '0.0.0.0', () => console.log("🚀 SERVER RUNNING on port " + PORT))
