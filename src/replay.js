// Phase 2: replays plan.json in order, each message sent by its original sender.
//
// Per message (state saved to disk after every step):
//   send   -> sender sends it with a deterministic id (a resend never duplicates)
//          -> wait until the other account actually receives it (ordering barrier)
//   recv   -> queue "delete for me" of A's new copy (A already has the original)
//             and move on; a background worker drains the queue (persisted too)
//
// Usage: node src/replay.js [--max N]
import fs from 'node:fs'
import { proto } from 'baileys'
import {
  P, Fatal, FATAL_EXIT, connect, endStatus, loadPlan, log, logErr, mediaFile, newId, norm, num, readJSON,
  saveSent, setStatus, sleep, toNum, writeJSON,
} from './common.js'
import * as breaker from './breaker.js'

const DELAY = Number(process.env.DELAY_MS || 500) // pause between messages
const DELETE_DELAY = Number(process.env.DELETE_DELAY_MS || 5000) // let A's phone get it before deleting
const NO_DELETE = process.env.NO_DELETE === '1' // keep duplicates on A (e.g. A will restore a backup afterwards)
const MAX_PENDING_DELETES = 200
const RETRY_BASE_S = Number(process.env.RETRY_BASE_S || 5)
const RECV_TIMEOUT = Number(process.env.RECV_TIMEOUT_S || 90) * 1000
const SKIP_MISSING = process.env.SKIP_MISSING_MEDIA === '1'
const FORCE = process.env.FORCE_CONTINUE === '1'
const ai = process.argv.indexOf('--max')
const MAX = ai > 0 ? Number(process.argv[ai + 1]) : Infinity

const plan = loadPlan()
const items = plan.items
const byOld = new Map(items.map((it) => [it.oldId, it]))
const planIds = new Set(items.map((it) => it.id))
let state = readJSON(P.state, { next: 0, cur: null })
state.deletes ??= [] // [{id, key, ts, at}] waiting to be deleted on A
const save = () => writeJSON(P.state, state)

// ---- receipts: which replayed ids each side has received (persisted) ------

const RECV = P.state.replace('state.json', 'recv.jsonl')
const recv = { a: new Map(), b: new Map() }
if (fs.existsSync(RECV)) {
  for (const l of fs.readFileSync(RECV, 'utf8').split('\n')) if (l.trim()) {
    const r = JSON.parse(l)
    recv[r.side].set(r.id, r)
  }
}
function trackReceipts(side, sock) {
  sock.ev.on('messages.upsert', ({ messages }) => {
    for (const m of messages) {
      const id = m.key?.id
      if (!planIds.has(id) || m.key.fromMe || recv[side].has(id)) continue
      if (!m.message || m.messageStubType === proto.WebMessageInfo.StubType.CIPHERTEXT) continue // retry pending
      const r = { side, id, key: m.key, ts: toNum(m.messageTimestamp) }
      recv[side].set(id, r)
      fs.appendFileSync(RECV, JSON.stringify(r) + '\n')
    }
  })
}
async function waitRecv(side, id, ms) {
  const t0 = Date.now()
  while (Date.now() - t0 < ms) {
    if (lost) throw lost
    if (recv[side].has(id)) return recv[side].get(id)
    await sleep(300)
  }
  return null
}

// ---- connection --------------------------------------------------------

let lost = null
const onLost = (e) => { lost ??= e }

// ---- message building ----------------------------------------------------

const clean = (o) => Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined))
const buf64 = (s) => (s ? Buffer.from(s, 'base64') : undefined)

function content(it, to) {
  const o = it.opts || {}
  const media = () => fs.readFileSync(mediaFile(it.oldId))
  const thumb = buf64(it.thumb)
  switch (it.type) {
    case 'text': return { text: it.text }
    case 'image': return clean({ image: media(), caption: o.caption, mimetype: o.mimetype, jpegThumbnail: thumb, width: o.width, height: o.height })
    case 'video': return clean({ video: media(), caption: o.caption, mimetype: o.mimetype, gifPlayback: o.gifPlayback, jpegThumbnail: thumb, width: o.width, height: o.height, seconds: o.seconds })
    case 'ptv': return clean({ video: media(), ptv: true, mimetype: o.mimetype, jpegThumbnail: thumb, seconds: o.seconds })
    case 'audio': return clean({ audio: media(), mimetype: o.mimetype, ptt: o.ptt, seconds: o.seconds, waveform: buf64(it.waveform) })
    case 'sticker': return clean({ sticker: media(), mimetype: o.mimetype, isAnimated: o.isAnimated, width: o.width, height: o.height })
    case 'document': return clean({ document: media(), mimetype: o.mimetype || 'application/octet-stream', fileName: o.fileName, caption: o.caption, jpegThumbnail: thumb })
    case 'location': return { location: clean(it.location) }
    case 'contacts': return { contacts: it.contacts }
    case 'poll': return { poll: it.poll }
    case 'reaction': {
      const t = byOld.get(it.target)
      return { react: { text: it.text, key: { remoteJid: to, fromMe: t.from === it.from, id: t.id } } }
    }
  }
  throw new Fatal(`tipo desconhecido no plano: ${it.type}`)
}

function quoted(it, to) {
  const t = it.quote && byOld.get(it.quote.oldId)
  if (!t || t.n >= it.n) return undefined
  return { key: { remoteJid: to, fromMe: t.from === it.from, id: t.id }, message: proto.Message.decode(Buffer.from(it.quote.msg, 'base64')) }
}

const preview = (it) => {
  const s = it.type === 'text' ? it.text : `[${it.type}] ${it.opts?.caption || it.text || ''}`
  return JSON.stringify(s.replace(/\s+/g, ' ').slice(0, 60))
}

// ---- main ----------------------------------------------------------------

// ---- progress -----------------------------------------------------------

const fmt = (n) => Math.round(n).toLocaleString('pt-BR')
const dur = (s) => (s >= 3600 ? `${Math.floor(s / 3600)}h${String(Math.floor((s % 3600) / 60)).padStart(2, '0')}` : `${Math.floor(s / 60)}m${String(Math.floor(s % 60)).padStart(2, '0')}s`)
const prog = { sent: { a: 0, b: 0 }, session: 0, startedAt: Date.now(), recent: [], phase: '' }
for (const it of items.slice(0, state.next)) prog.sent[it.from]++

function statusLine() {
  const done = state.next
  const pct = ((done / items.length) * 100).toFixed(2).replace('.', ',')
  // rate over the last 200 messages (reacts faster than the session average)
  const r = prog.recent
  const rate = r.length > 1 ? (r.length - 1) / ((r.at(-1) - r[0]) / 1000) : 0
  const eta = rate ? dur((items.length - done) / rate) : '?'
  const errs = Math.max(breaker.count('envio'), breaker.count('exclusão'))
  return [
    `enviadas ${fmt(done)}/${fmt(items.length)} (${pct}%)`,
    `A→B ${fmt(prog.sent.a)} B→A ${fmt(prog.sent.b)}`,
    `sessão +${fmt(prog.session)} em ${dur((Date.now() - prog.startedAt) / 1000)}`,
    `${rate.toFixed(2).replace('.', ',')} msg/s`,
    `faltam ~${eta}`,
    `excl. pendentes ${state.deletes.length}`,
    `erros seguidos ${errs}/${breaker.LIMIT}`,
    prog.phase,
  ].filter(Boolean).join(' │ ')
}
function tick(it) {
  prog.sent[it.from]++
  prog.session++
  prog.recent.push(Date.now())
  if (prog.recent.length > 200) prog.recent.shift()
}

async function main() {
  breaker.check()
  if (state.next >= items.length) return log('Tudo já foi enviado.')

  const missing = items.slice(state.next).filter((it) => it.missing)
  if (missing.length && !SKIP_MISSING) {
    throw new Fatal(`${missing.length} mídia(s) não foram baixadas (ex: #${missing[0].n}). Rode o export de novo, ou SKIP_MISSING_MEDIA=1 para pular.`)
  }

  const { sock: A } = await connect('a', { label: 'A (quem TEM o histórico)', onSocket: (s) => trackReceipts('a', s), onLost })
  const { sock: B } = await connect('b', { label: 'B (quem PERDEU o histórico)', onSocket: (s) => trackReceipts('b', s), onLost })
  const jidA = norm(A.user.id)
  const jidB = norm(B.user.id)

  // anti-leak: both sessions must be exactly the two people in the plan
  if (jidA !== plan.owner) throw new Fatal(`Sessão A é ${num(jidA)}, mas o plano é de ${num(plan.owner)}. Apague data/auth/a e pareie o celular certo.`)
  if (jidB !== plan.partner) throw new Fatal(`Sessão B é ${num(jidB)}, mas o plano espera ${num(plan.partner)}. Apague data/auth/b e pareie o celular certo.`)
  const peer = { a: { sock: A, to: jidB, other: 'b' }, b: { sock: B, to: jidA, other: 'a' } }

  async function send(it) {
    for (let t = 0; ; t++) {
      try {
        return await sendOnce(it)
      } catch (e) {
        if (e instanceof Fatal || lost) throw e
        const n = breaker.fail('envio', e, `#${it.n + 1} ${it.from === 'a' ? 'A→B' : 'B→A'} [${it.type}]`)
        const wait = Math.min(300, RETRY_BASE_S * 2 ** t)
        log(`#${it.n + 1} erro no envio (${n}/${breaker.LIMIT}): ${e.message}; nova tentativa em ${wait}s`)
        await sleep(wait * 1000)
      }
    }
  }

  async function sendOnce(it) {
    const { sock, to } = peer[it.from]
    if (to !== (it.from === 'a' ? plan.partner : plan.owner)) throw new Fatal('destinatário inesperado, abortando')
    const msg = await sock.sendMessage(to, content(it, to), { messageId: it.id, quoted: quoted(it, to), mediaUploadTimeoutMs: 10 * 60_000 })
    if (msg.key.id !== it.id) throw new Fatal(`envio inconsistente em #${it.n}`)
    saveSent(msg)
    return msg
  }

  // background worker: delete A's copies "for me", oldest first
  let stopWorker = false
  const worker = (async () => {
    while (!stopWorker || state.deletes.length) {
      const d = state.deletes[0]
      if (!d) { await sleep(300); continue }
      const wait = d.at + DELETE_DELAY - Date.now()
      if (wait > 0) { await sleep(Math.min(wait, 300)); continue }
      if (lost) return
      try {
        await A.chatModify({ deleteForMe: { key: d.key, timestamp: d.ts, deleteMedia: false } }, d.key.remoteJid)
        state.deletes.shift()
        save()
        setStatus(statusLine())
      } catch (e) {
        if (lost) return
        try {
          const n = breaker.fail('exclusão', e, `apagar #${d.n + 1} em A`)
          log(`apagar #${d.n + 1} em A falhou (${n}/${breaker.LIMIT}): ${e.message}; tentando de novo`)
        } catch (trip) {
          lost ??= trip // stops the main loop too
          return
        }
        await sleep(5000)
        continue
      }
      breaker.ok('exclusão')
    }
  })()

  let count = 0
  log(`retomando em #${state.next + 1} de ${items.length}${state.deletes.length ? `, ${state.deletes.length} exclusões pendentes` : ''}`)
  prog.startedAt = Date.now()
  const timer = setInterval(() => setStatus(statusLine()), 1000)
  while (state.next < items.length && count < MAX) {
    if (lost) throw lost
    const it = items[state.next]
    const tag = `#${it.n + 1}/${items.length} ${it.from === 'a' ? 'A→B' : 'B→A'}`
    if (it.id !== newId(it.oldId)) throw new Fatal('plan.json foi alterado')

    if (it.missing) {
      log(`${tag} PULADA (mídia ausente)`)
      state.next++
      state.cur = null
      save()
      continue
    }
    if (state.cur?.n !== it.n) {
      state.cur = { n: it.n, stage: 'send' }
      save()
    }
    const { other } = peer[it.from]

    if (state.cur.stage === 'send') {
      let got = recv[other].get(it.id) // may have arrived before a crash
      for (let attempt = 1; !got && attempt <= 3; attempt++) {
        if (attempt > 1) log(`${tag} reenviando (mesmo id, sem duplicar)`)
        prog.phase = `enviando #${it.n + 1} [${it.type}]`
        setStatus(statusLine())
        const msg = await send(it)
        if (it.from === 'a') {
          state.cur.aKey = msg.key
          state.cur.ts = toNum(msg.messageTimestamp)
          save()
        }
        prog.phase = `aguardando ${other.toUpperCase()} receber #${it.n + 1}`
        got = await waitRecv(other, it.id, RECV_TIMEOUT)
        if (!got) {
          const n = breaker.fail('envio', new Error(`${other.toUpperCase()} não confirmou o recebimento em ${RECV_TIMEOUT / 1000}s`), tag)
          log(`${tag} sem confirmação de recebimento (${n}/${breaker.LIMIT})`)
        }
      }
      prog.phase = ''
      if (!got && !FORCE) {
        throw new Fatal(`${tag} não chegou em ${other.toUpperCase()} após 3 tentativas. Confira os celulares; se a mensagem apareceu, rode com FORCE_CONTINUE=1.`)
      }
      if (got && it.from === 'b') {
        state.cur.aKey = got.key // the copy as A's account sees it
        state.cur.ts = got.ts
      }
      if (got && it.from === 'a') B.readMessages([got.key]).catch(() => {})
      state.cur.stage = 'recv'
      save()
    }

    if (state.cur.stage === 'recv') {
      while (state.deletes.length >= MAX_PENDING_DELETES) {
        if (lost) throw lost
        await sleep(300)
      }
      if (it.type !== 'reaction' && !NO_DELETE) {
        const k = state.cur.aKey || { remoteJid: jidB, fromMe: it.from === 'a', id: it.id }
        state.deletes.push({ n: it.n, key: { remoteJid: k.remoteJid, fromMe: !!k.fromMe, id: it.id }, ts: state.cur.ts || Math.floor(Date.now() / 1000), at: Date.now() })
      }
      state.next++
      state.cur = null
      save() // queue + advance in one atomic write
      count++
      breaker.ok('envio')
      tick(it)
      log(`${tag} ok ${preview(it)}`)
      if (count % 100 === 0) log(`---- ${statusLine()} ----`)
      setStatus(statusLine())
    }
    await sleep(DELAY)
  }

  prog.phase = state.deletes.length ? 'finalizando exclusões em A' : ''
  if (state.next >= items.length) log('==== RECUPERAÇÃO CONCLUÍDA ====')
  else log(`parado em #${state.next + 1} (--max ${MAX}). Rode de novo para continuar.`)
  if (state.deletes.length) log(`aguardando ${state.deletes.length} exclusões em A...`)
  stopWorker = true
  await worker
  if (lost) throw lost
  await sleep(3000) // let pending receipts flush
  clearInterval(timer)
  endStatus()
  A.end(undefined)
  B.end(undefined)
}

process.on('SIGINT', () => {
  endStatus()
  console.log('\ninterrompido; rode de novo para retomar.')
  process.exit(130)
})
process.on('unhandledRejection', (e) => logErr('unhandledRejection:', e?.message || e))

main().then(
  () => process.exit(0),
  (e) => {
    endStatus()
    if (!(e instanceof Fatal)) {
      // crash / lost connection: counts towards the breaker across automatic restarts
      try {
        const n = breaker.fail('envio', e, `processo (msg #${state.next + 1})`)
        console.error(`ERRO (${n}/${breaker.LIMIT}): ${e.message}`)
      } catch (trip) {
        e = trip
      }
    }
    if (e instanceof breaker.Tripped) console.error(e.message)
    else if (e instanceof Fatal) console.error('ERRO:', e.message)
    process.exit(e instanceof Fatal ? FATAL_EXIT : 1)
  },
)
