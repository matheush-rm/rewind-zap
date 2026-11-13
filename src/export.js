// Phase 1: connect A (the account that still has the history), capture the
// conversation with B, download every media file and write a frozen plan.json.
//
// Safe to run more than once (it merges and only downloads missing media) as
// long as the replay has not started.
import fs from 'node:fs'
import { pipeline } from 'node:stream/promises'
import { downloadMediaMessage, getContentType, normalizeMessageContent, proto } from 'baileys'
import {
  P, Fatal, FATAL_EXIT, connect, decWMI, encWMI, log, logger, mediaFile, newId, norm, num,
  readJSON, sleep, toNum, writeJSON,
} from './common.js'

const HT = proto.HistorySync.HistorySyncType
const MEDIA_CONC = Number(process.env.MEDIA_CONCURRENCY || 3)
const IDLE_S = Number(process.env.HISTORY_IDLE_S || 300)

const partnerDigits = (process.env.PARTNER || '').replace(/\D/g, '')

// Brazilian numbers may or may not carry the extra 9 in their WhatsApp id.
function pnVariants(d) {
  const v = new Set([d])
  if (d.startsWith('55') && d.length === 13 && d[4] === '9') v.add(d.slice(0, 4) + d.slice(5))
  if (d.startsWith('55') && d.length === 12) v.add(d.slice(0, 4) + '9' + d.slice(4))
  return [...v].map((x) => x + '@s.whatsapp.net')
}
const partnerJids = new Set(pnVariants(partnerDigits))

// ---- raw store (one line per message, survives crashes) ------------------

const raw = new Map() // oldId -> { chunk, pos, ts, jid, b64 }
if (fs.existsSync(P.raw)) {
  for (const line of fs.readFileSync(P.raw, 'utf8').split('\n')) {
    if (!line.trim()) continue
    const r = JSON.parse(line)
    raw.set(r.id, r)
  }
  log(`raw.jsonl: ${raw.size} mensagens já capturadas`)
}
let chunkSeq = Math.max(0, ...[...raw.values()].map((r) => r.chunk)) + 1
const heldLid = new Map() // lid jid -> [{m,pos,chunk}] not yet known to belong to B (memory only)

function store(m, chunk, pos) {
  const id = m.key?.id
  if (!id || raw.has(id)) return false
  const r = { id, chunk, pos, ts: toNum(m.messageTimestamp), jid: m.key.remoteJid, b64: encWMI(m) }
  raw.set(id, r)
  fs.appendFileSync(P.raw, JSON.stringify(r) + '\n')
  return true
}

function learn(lid, pn) {
  if (lid && pn && partnerJids.has(norm(pn)) && !partnerJids.has(norm(lid))) {
    partnerJids.add(norm(lid))
    log(`B também identificado como ${lid}`)
    for (const h of heldLid.get(norm(lid)) || []) store(h.m, h.chunk, h.pos)
    heldLid.delete(norm(lid))
  }
}

const isB = (m) => partnerJids.has(norm(m.key.remoteJid)) || (m.key.remoteJidAlt && partnerJids.has(norm(m.key.remoteJidAlt)))

let lastEvent = Date.now()
let fullDone = false
let onDemandHits = 0

function onHistory({ chats, messages, lidPnMappings, syncType, progress }) {
  lastEvent = Date.now()
  for (const mp of lidPnMappings || []) learn(mp.lid, mp.pn)
  for (const c of chats || []) {
    if (c.pnJid) learn(c.id, c.pnJid)
    if (c.lidJid) learn(c.lidJid, c.id)
  }
  const chunk = chunkSeq++
  let added = 0
  messages.forEach((m, pos) => {
    const jid = m.key?.remoteJid || ''
    if (isB(m)) {
      if (store(m, chunk, pos)) added++
    } else if (jid.endsWith('@lid') && ![...partnerJids].some((j) => j.endsWith('@lid'))) {
      // B's LID still unknown: hold @lid chats in memory until it is (stops once known)
      const k = norm(jid)
      if (!heldLid.has(k)) heldLid.set(k, [])
      heldLid.get(k).push({ m, chunk, pos })
    }
  })
  if (syncType === HT.ON_DEMAND) onDemandHits += added + 1
  log(`histórico ${HT[syncType] ?? syncType} ${progress ?? '-'}%: +${added} (total ${raw.size})`)
}

// ---- parse raw messages into plan items ----------------------------------

const MEDIA = { imageMessage: 'image', videoMessage: 'video', ptvMessage: 'ptv', audioMessage: 'audio', stickerMessage: 'sticker', documentMessage: 'document' }
const b64 = (b) => (b && b.length ? Buffer.from(b).toString('base64') : undefined)

function parse(m) {
  const c = normalizeMessageContent(m.message)
  if (!c) return { skip: m.messageStubType ? `aviso do sistema (stub ${m.messageStubType})` : 'sem conteúdo' }
  const type = getContentType(c)
  const body = c[type]
  const item = { type }

  if (type === 'conversation') return { type: 'text', text: c.conversation }
  if (type === 'extendedTextMessage') Object.assign(item, { type: 'text', text: body.text })
  else if (MEDIA[type]) {
    item.type = MEDIA[type]
    item.opts = {
      mimetype: body.mimetype || undefined,
      caption: body.caption || undefined,
      fileName: body.fileName || undefined,
      ptt: body.ptt || undefined,
      seconds: body.seconds || undefined,
      gifPlayback: body.gifPlayback || undefined,
      isAnimated: body.isAnimated || undefined,
      width: body.width || undefined,
      height: body.height || undefined,
    }
    item.thumb = b64(body.jpegThumbnail)
    item.waveform = b64(body.waveform)
  } else if (type === 'locationMessage' || type === 'liveLocationMessage') {
    item.type = 'location'
    item.location = { degreesLatitude: body.degreesLatitude, degreesLongitude: body.degreesLongitude, name: body.name || undefined, address: body.address || undefined }
  } else if (type === 'contactMessage') {
    item.type = 'contacts'
    item.contacts = { displayName: body.displayName, contacts: [{ displayName: body.displayName, vcard: body.vcard }] }
  } else if (type === 'contactsArrayMessage') {
    item.type = 'contacts'
    item.contacts = { displayName: body.displayName, contacts: body.contacts.map((x) => ({ displayName: x.displayName, vcard: x.vcard })) }
  } else if (/^pollCreationMessage(V\d)?$/.test(type)) {
    item.type = 'poll'
    item.poll = { name: body.name, values: body.options.map((o) => o.optionName), selectableCount: body.selectableOptionsCount || 0 }
  } else if (type === 'reactionMessage') {
    return { type: 'reaction', target: body.key?.id, text: body.text }
  } else if (type === 'protocolMessage') {
    const T = proto.Message.ProtocolMessage.Type
    if (body.type === T.REVOKE) return { type: 'revoke', target: body.key?.id }
    if (body.type === T.MESSAGE_EDIT) return { type: 'edit', target: body.key?.id, edited: body.editedMessage }
    return { skip: `protocolMessage ${body.type}` }
  } else return { skip: `tipo não suportado: ${type}` }

  const ctx = body?.contextInfo
  if (ctx?.stanzaId && ctx.quotedMessage) {
    item.quote = { oldId: ctx.stanzaId, msg: Buffer.from(proto.Message.encode(ctx.quotedMessage).finish()).toString('base64') }
  }
  return item
}

function applyEdit(item, edited) {
  const e = normalizeMessageContent(edited)
  if (!e) return
  const t = getContentType(e)
  const text = t === 'conversation' ? e.conversation : e[t]?.text ?? e[t]?.caption
  if (text == null) return
  if (item.type === 'text') item.text = text
  else if (item.opts) item.opts.caption = text
}

function buildPlan(owner, partner) {
  const recs = [...raw.values()].map((r) => ({ r, m: decWMI(r.b64) }))
  // chronological; same second: history chunks list newest first, so higher pos = older
  recs.sort((x, y) => x.r.ts - y.r.ts || y.r.chunk - x.r.chunk || y.r.pos - x.r.pos)

  const items = []
  const byOld = new Map()
  const skipped = []
  const reactions = new Map() // `${target}|${from}` -> {ts,text,...}
  const jidCount = {}

  for (const { r, m } of recs) {
    jidCount[r.jid] = (jidCount[r.jid] || 0) + 1
    const from = m.key.fromMe ? 'a' : 'b'
    const p = parse(m)
    if (p.skip) { skipped.push({ oldId: r.id, ts: r.ts, from, reason: p.skip }); continue }
    if (p.type === 'revoke') { byOld.get(p.target) && (byOld.get(p.target).revoked = true); continue }
    if (p.type === 'edit') { byOld.get(p.target) && applyEdit(byOld.get(p.target), p.edited); continue }
    if (p.type === 'reaction') {
      reactions.set(`${p.target}|${from}`, { ts: r.ts, target: p.target, text: p.text, from })
      continue
    }
    if (m.messageStubType && !m.message) { skipped.push({ oldId: r.id, ts: r.ts, from, reason: 'stub' }); continue }
    const it = { oldId: r.id, from, ts: r.ts, ...p }
    items.push(it)
    byOld.set(r.id, it)
    // reactions stored inline on the message (how history sync usually delivers them)
    for (const rc of m.reactions || []) {
      const rf = rc.key?.fromMe ? 'a' : 'b'
      const ts = Math.floor(toNum(rc.senderTimestampMs) / 1000) || r.ts
      const k = `${r.id}|${rf}`
      if (!reactions.has(k) || reactions.get(k).ts <= ts) reactions.set(k, { ts, target: r.id, text: rc.text, from: rf })
    }
  }
  if (Object.keys(jidCount).length) log('conversas capturadas (jid: msgs):', jidCount)

  // edits can make a message no longer equal; revoked ones are dropped
  let final = items.filter((it) => {
    if (it.revoked) skipped.push({ oldId: it.oldId, ts: it.ts, from: it.from, reason: 'apagada para todos' })
    return !it.revoked
  })
  // merge reactions chronologically, always after their target
  const pos = new Map(final.map((it, i) => [it.oldId, i]))
  const reacts = [...reactions.values()].filter((x) => x.text && pos.has(x.target))
  const seq = final.map((it, i) => ({ it, key: [it.ts, i, 0] }))
  for (const x of reacts) {
    const i = pos.get(x.target)
    seq.push({ it: { oldId: `react:${x.target}:${x.from}`, from: x.from, ts: x.ts, type: 'reaction', target: x.target, text: x.text }, key: [Math.max(x.ts, final[i].ts), i, 1] })
  }
  seq.sort((a, b) => a.key[0] - b.key[0] || a.key[1] - b.key[1] || a.key[2] - b.key[2])
  final = seq.map((s, n) => ({ n, id: newId(s.it.oldId), ...s.it }))

  return {
    createdAt: new Date().toISOString(),
    owner: norm(owner),
    partner: norm(partner),
    partnerJids: [...partnerJids],
    items: final,
    skipped,
  }
}

// ---- main ----------------------------------------------------------------

async function main() {
  if (!partnerDigits) throw new Fatal('Defina PARTNER com o número de quem perdeu as conversas (ex: PARTNER=5511999998888)')
  const st = readJSON(P.state)
  if (st && (st.next > 0 || st.cur)) throw new Fatal('O replay já começou (data/state.json). O plano está congelado; não rode o export de novo.')
  let lost = null
  const { sock, fresh } = await connect('a', {
    label: 'A (quem TEM o histórico)',
    history: true,
    onSocket: (s) => s.ev.on('messaging-history.set', onHistory),
    onLost: (e) => (lost = e),
  })
  sock.ev.on('messaging-history.status', ({ syncType, status }) => {
    log(`sync ${HT[syncType] ?? syncType}: ${status}`)
    if (syncType === HT.FULL && status === 'complete') fullDone = true
  })

  // confirm B's real jid and LID
  const [res] = await sock.onWhatsApp(partnerDigits)
  if (!res?.exists) throw new Fatal(`${partnerDigits} não está no WhatsApp`)
  const partner = norm(res.jid)
  partnerJids.add(partner)
  const lid = await sock.signalRepository.lidMapping.getLIDForPN(partner).catch(() => null)
  if (lid) learn(lid, partner)
  if (res.lid) learn(res.lid, partner)
  if (norm(sock.user.id) === partner) throw new Fatal('PARTNER é o próprio número de A. Escaneou com o celular errado?')

  // 1) wait for the initial history sync (only happens on the first pairing)
  log(fresh ? `aguardando histórico (encerra após ${IDLE_S}s sem novidades)...` : 'sessão já pareada: o histórico completo só vem no 1º pareamento; vou buscar mais sob demanda.')
  const idle = fresh ? IDLE_S : 15
  while (!lost && !(fullDone && Date.now() - lastEvent > 15000) && Date.now() - lastEvent < idle * 1000) await sleep(1000)
  if (lost) throw lost

  // 2) ask the phone for older messages until nothing new comes back
  let empty = 0
  while (raw.size && empty < 2 && !lost) {
    let oldest
    for (const r of raw.values()) {
      if (!oldest || r.ts < oldest.ts || (r.ts === oldest.ts && (r.chunk > oldest.chunk || (r.chunk === oldest.chunk && r.pos > oldest.pos)))) oldest = r
    }
    const m = decWMI(oldest.b64)
    const before = raw.size
    onDemandHits = 0
    try {
      await sock.fetchMessageHistory(50, m.key, oldest.ts)
    } catch (e) {
      log('fetchMessageHistory falhou:', e.message)
      break
    }
    const t0 = Date.now()
    while (!onDemandHits && Date.now() - t0 < 30000) await sleep(500)
    await sleep(3000)
    const got = raw.size - before
    log(`sob demanda: +${got} mais antigas`)
    empty = got ? 0 : empty + 1
  }
  if (lost) throw lost
  if (heldLid.size) log(`aviso: ${heldLid.size} conversa(s) @lid não associadas a B foram ignoradas`)
  heldLid.clear()
  if (!raw.size) throw new Fatal('Nenhuma mensagem com B foi encontrada no histórico.')

  // 3) plan + media
  const plan = buildPlan(sock.user.id, partner)
  const needMedia = plan.items.filter((it) => ['image', 'video', 'ptv', 'audio', 'sticker', 'document'].includes(it.type))
  let failed = 0
  let done = 0
  const todo = needMedia.filter((it) => !fs.existsSync(mediaFile(it.oldId)))
  log(`mídias: ${needMedia.length} no total, ${todo.length} para baixar`)
  async function download(it) {
    const f = mediaFile(it.oldId)
    const m = decWMI(raw.get(it.oldId).b64)
    for (let t = 1; t <= 3; t++) {
      try {
        const stream = await downloadMediaMessage(m, 'stream', {}, { logger, reuploadRequest: sock.updateMediaMessage })
        await pipeline(stream, fs.createWriteStream(f + '.tmp'))
        fs.renameSync(f + '.tmp', f)
        return true
      } catch (e) {
        if (lost) throw lost
        if (t === 3) log(`mídia #${it.n + 1} (${it.type}) falhou: ${e.message}`)
        else await sleep(2000 * t)
      }
    }
    return false
  }
  const queue = [...todo]
  await Promise.all(Array.from({ length: MEDIA_CONC }, async () => {
    for (let it; (it = queue.shift()); ) {
      if (!(await download(it))) failed++
      if (++done % 50 === 0) log(`mídias: ${done}/${todo.length} (${failed} falhas)`)
    }
  }))
  for (const it of needMedia) it.missing = !fs.existsSync(mediaFile(it.oldId)) || undefined
  writeJSON(P.plan, plan)

  const by = {}
  for (const it of plan.items) by[`${it.from === 'a' ? 'A→B' : 'B→A'} ${it.type}`] = (by[`${it.from === 'a' ? 'A→B' : 'B→A'} ${it.type}`] || 0) + 1
  const sk = {}
  for (const s of plan.skipped) sk[s.reason] = (sk[s.reason] || 0) + 1
  console.log('\n==== plano gerado: data/plan.json ====')
  console.log(`A = ${num(plan.owner)}   B = ${num(plan.partner)}`)
  console.log(`mensagens a reenviar: ${plan.items.length}`, by)
  console.log('ignoradas:', sk)
  if (plan.items.length) {
    const d = (s) => new Date(s * 1000).toLocaleString('pt-BR')
    console.log(`período: ${d(plan.items[0].ts)} → ${d(plan.items.at(-1).ts)}`)
  }
  if (failed) console.log(`\n!! ${failed} mídia(s) não baixaram. Rode o export de novo com o celular de A online.`)
  console.log('\nConfira com: npm run preview  (ou docker compose run --rm app preview)')
  sock.end(undefined)
}

export { buildPlan, onHistory, raw }

if (import.meta.url === `file://${process.argv[1]}`) main().then(
  () => process.exit(0),
  (e) => {
    console.error('ERRO:', e.message)
    process.exit(e instanceof Fatal ? FATAL_EXIT : 1)
  },
)
