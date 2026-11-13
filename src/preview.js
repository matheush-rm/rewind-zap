// Prints the plan as a transcript so it can be checked before (or during) the replay.
// Usage: node src/preview.js [--skipped]
import { P, loadPlan, num, readJSON } from './common.js'

const plan = loadPlan()
const state = readJSON(P.state, { next: 0 })
const d = (s) => new Date(s * 1000).toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' })
const nOf = new Map(plan.items.map((x) => [x.oldId, x.n + 1]))
const who = { a: `A(${num(plan.owner)})`, b: `B(${num(plan.partner)})` }

for (const it of plan.items) {
  const mark = it.n < state.next ? '✓' : it.n === state.next ? '▶' : ' '
  let s
  if (it.type === 'text') s = it.text
  else if (it.type === 'reaction') s = `[reação ${it.text} em #${nOf.get(it.target)}]`
  else s = `[${it.type}${it.missing ? ' SEM ARQUIVO' : ''}] ${it.opts?.caption || it.opts?.fileName || it.location?.name || it.poll?.name || ''}`
  const q = it.quote ? ` ↩#${nOf.get(it.quote.oldId) ?? '?'}` : ''
  console.log(`${mark} #${it.n + 1} ${d(it.ts)} ${who[it.from]}${q}: ${s.replace(/\s+/g, ' ').slice(0, 120)}`)
}
console.log(`\n${plan.items.length} mensagens, ${state.next} já enviadas.`)

if (process.argv.includes('--skipped')) {
  console.log('\nIgnoradas:')
  for (const s of plan.skipped) console.log(`  ${d(s.ts)} ${who[s.from]}: ${s.reason}`)
} else console.log(`${plan.skipped.length} ignoradas (veja com --skipped).`)
