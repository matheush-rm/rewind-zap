// Progress snapshot from the files on disk. Safe to run while the replay is running.
import fs from 'node:fs'
import path from 'node:path'
import { DATA, P, loadPlan, readJSON } from './common.js'

const plan = loadPlan()
const state = readJSON(P.state, { next: 0, deletes: [] })
const br = readJSON(path.join(DATA, 'breaker.json'), {})
const items = plan.items
const fmt = (n) => n.toLocaleString('pt-BR')

const sent = { a: 0, b: 0 }
const types = {}
for (const it of items.slice(0, state.next)) {
  sent[it.from]++
  types[it.type] = (types[it.type] || 0) + 1
}
const left = items.length - state.next
const mtime = fs.existsSync(P.state) ? fs.statSync(P.state).mtime : null
const ago = mtime ? Math.round((Date.now() - mtime) / 1000) : null

console.log(`enviadas:   ${fmt(state.next)} de ${fmt(items.length)} (${((state.next / items.length) * 100).toFixed(2).replace('.', ',')}%)`)
console.log(`  A→B ${fmt(sent.a)}   B→A ${fmt(sent.b)}`)
console.log(`  por tipo: ${Object.entries(types).map(([k, v]) => `${k} ${fmt(v)}`).join(', ') || '-'}`)
console.log(`faltam:     ${fmt(left)}`)
console.log(`exclusões pendentes em A: ${state.deletes?.length || 0}`)
if (state.cur) console.log(`em andamento: #${state.cur.n + 1} (etapa ${state.cur.stage})`)
if (ago != null) console.log(`última atividade: há ${ago}s${ago > 300 ? '  (parado?)' : ''}`)
for (const [kind, s] of Object.entries(br)) {
  if (!s.n) continue
  const e = s.errors.at(-1)
  console.log(`erros seguidos (${kind}): ${s.n}   último: ${e.where}: ${e.message}`)
}
if (state.next >= items.length && !state.deletes?.length) console.log('\nRECUPERAÇÃO CONCLUÍDA')
