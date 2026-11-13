import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import pino from 'pino'
import qrcode from 'qrcode-terminal'
import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  jidNormalizedUser,
  proto,
  useMultiFileAuthState,
} from 'baileys'

export const DATA = path.resolve(process.env.DATA_DIR || 'data')
export const P = {
  auth: (name) => path.join(DATA, 'auth', name),
  raw: path.join(DATA, 'raw.jsonl'),
  plan: path.join(DATA, 'plan.json'),
  state: path.join(DATA, 'state.json'),
  media: path.join(DATA, 'media'),
  sent: path.join(DATA, 'sent'),
}
for (const d of [DATA, P.media, P.sent]) fs.mkdirSync(d, { recursive: true })

export const logger = pino({ level: process.env.LOG_LEVEL || 'warn' })
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
export const hhmmss = (d = new Date()) => d.toLocaleTimeString('pt-BR', { hour12: false })

// ---- status bar: one line pinned at the bottom of the terminal -------------
// log() clears it, prints above it and draws it again.

const TTY = process.stdout.isTTY
let statusText = ''
const drawStatus = () => {
  if (!TTY || !statusText) return
  const cols = process.stdout.columns || 120
  process.stdout.write('\r\x1b[2K\x1b[7m' + statusText.slice(0, cols - 1) + '\x1b[0m')
}
const clearStatus = () => TTY && statusText && process.stdout.write('\r\x1b[2K')
export function setStatus(text) {
  statusText = text
  drawStatus()
}
export function endStatus() {
  clearStatus()
  if (TTY && statusText) console.log(statusText)
  statusText = ''
}
export const log = (...a) => {
  clearStatus()
  console.log(hhmmss(), ...a)
  drawStatus()
}
export const logErr = (...a) => {
  clearStatus()
  console.error(...a)
  drawStatus()
}

/** Fatal error: the wrapper script must NOT retry automatically. */
export class Fatal extends Error {}
export const FATAL_EXIT = 3

export const num = (jid) => (jid || '').split('@')[0].split(':')[0]
export const norm = (jid) => jidNormalizedUser(jid)
export const toNum = (v) => (v == null ? 0 : typeof v === 'number' ? v : Number(v.toString()))

// ---- persistence ---------------------------------------------------------

export function readJSON(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch (e) {
    if (e.code === 'ENOENT') return fallback
    throw e
  }
}

/** Atomic write: tmp + fsync + rename, so a crash never leaves a half-written file. */
export function writeJSON(file, data) {
  const tmp = file + '.tmp'
  const fd = fs.openSync(tmp, 'w')
  fs.writeSync(fd, JSON.stringify(data, null, 2))
  fs.fsyncSync(fd)
  fs.closeSync(fd)
  fs.renameSync(tmp, file)
}

export const encWMI = (m) => Buffer.from(proto.WebMessageInfo.encode(m).finish()).toString('base64')
export const decWMI = (b64) => proto.WebMessageInfo.decode(Buffer.from(b64, 'base64'))

export const mediaFile = (oldId) =>
  path.join(P.media, crypto.createHash('sha1').update(oldId).digest('hex'))

/**
 * Deterministic id for the replayed message. Re-sending after a crash uses the
 * same id, and WhatsApp drops duplicates by id, so a resend never duplicates.
 */
export const newId = (seed) =>
  '3EB0' + crypto.createHash('sha256').update('rewind-zap:' + seed).digest('hex').slice(0, 20).toUpperCase()

// sent message store, used to answer retry requests (getMessage)
export function saveSent(msg) {
  if (!msg?.key?.id || !msg.message) return
  const b64 = Buffer.from(proto.Message.encode(msg.message).finish()).toString('base64')
  fs.writeFileSync(path.join(P.sent, msg.key.id), b64)
}
async function getMessage(key) {
  try {
    const b64 = fs.readFileSync(path.join(P.sent, path.basename(key.id)), 'utf8')
    return proto.Message.decode(Buffer.from(b64, 'base64'))
  } catch {
    return undefined
  }
}

// ---- connection ----------------------------------------------------------

/**
 * Connects a session and resolves once it is open. Handles the restart that
 * WhatsApp requests right after QR pairing. After open, any disconnect calls
 * onLost (the caller decides to exit and let the wrapper restart).
 */
export async function connect(name, { label, history = false, onSocket, onLost }) {
  const { version } = await fetchLatestBaileysVersion().catch(() => ({}))
  const { state, saveCreds } = await useMultiFileAuthState(P.auth(name))
  const fresh = !state.creds.registered

  for (;;) {
    const sock = makeWASocket({
      version,
      auth: state,
      logger,
      browser: Browsers.macOS('Desktop'), // "Desktop" is required for full history sync
      syncFullHistory: history,
      shouldSyncHistoryMessage: () => history,
      markOnlineOnConnect: false,
      getMessage,
    })
    sock.ev.on('creds.update', saveCreds)
    onSocket?.(sock)

    const result = await new Promise((resolve) => {
      let opened = false
      sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
        if (qr) {
          console.log(`\n>>> Escaneie com o WhatsApp de ${label} (Aparelhos conectados > Conectar aparelho):\n`)
          qrcode.generate(qr, { small: true })
        }
        if (connection === 'open') {
          opened = true
          resolve('open')
        }
        if (connection === 'close') {
          const code = lastDisconnect?.error?.output?.statusCode
          if (code === DisconnectReason.loggedOut) {
            const err = new Fatal(
              `Sessão de ${label} foi desconectada pelo celular. Apague ${P.auth(name)} e pareie de novo.`,
            )
            opened ? onLost?.(err) : resolve(err)
          } else if (opened) {
            onLost?.(new Error(`conexão de ${label} caiu (code ${code})`))
          } else {
            resolve('retry') // e.g. 515 restartRequired after pairing
          }
        }
      })
    })
    if (result === 'open') {
      log(`${label} conectado: ${num(sock.user.id)}`)
      return { sock, fresh }
    }
    if (result instanceof Error) throw result
    await sleep(2000)
  }
}

export function loadPlan() {
  const plan = readJSON(P.plan)
  if (!plan) throw new Fatal('data/plan.json não existe. Rode o export primeiro.')
  return plan
}
