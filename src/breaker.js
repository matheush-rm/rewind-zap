// Circuit breaker: counts consecutive errors per kind ("envio", "exclusão").
// Persisted in data/breaker.json so automatic restarts by run.sh keep counting;
// run.sh resets it when a human starts the replay again.
import path from 'node:path'
import { DATA, Fatal, hhmmss, readJSON, writeJSON } from './common.js'

export const LIMIT = Number(process.env.BREAKER_LIMIT || 10)
const FILE = path.join(DATA, 'breaker.json')

export class Tripped extends Fatal {}

const data = readJSON(FILE, {})
const save = () => writeJSON(FILE, data)
const slot = (kind) => (data[kind] ??= { n: 0, errors: [] })

export const count = (kind) => slot(kind).n

export function ok(kind) {
  const s = slot(kind)
  if (s.n === 0) return
  s.n = 0
  s.errors = []
  save()
}

/** Records an error; throws Tripped when the limit of consecutive errors is reached. */
export function fail(kind, err, where = '') {
  const s = slot(kind)
  s.n++
  s.errors.push({
    at: new Date().toISOString(),
    where,
    message: String(err?.message || err),
    status: err?.output?.statusCode ?? err?.data?.statusCode ?? err?.code ?? null,
    stack: String(err?.stack || '').split('\n').slice(1, 6).map((l) => l.trim()),
  })
  s.errors = s.errors.slice(-LIMIT)
  save()
  if (s.n >= LIMIT) throw new Tripped(report(kind))
  return s.n
}

/** Throws right away if a previous automatic run already tripped. */
export function check() {
  for (const kind of Object.keys(data)) if (data[kind].n >= LIMIT) throw new Tripped(report(kind))
}

function report(kind) {
  const s = slot(kind)
  const last = s.errors.at(-1)
  const t = (iso) => hhmmss(new Date(iso))
  const lines = [
    '',
    '═'.repeat(70),
    ` CIRCUIT BREAKER: ${s.n} erros seguidos em "${kind}". Replay PARADO.`,
    '═'.repeat(70),
    ` último erro (${t(last.at)}) ${last.where}`,
    `   ${last.message}${last.status != null ? `  [status ${last.status}]` : ''}`,
    ...last.stack.map((l) => `     ${l}`),
    '',
    ' erros anteriores:',
    ...s.errors.slice(0, -1).map((e) => `   ${t(e.at)} ${e.where}: ${e.message}${e.status != null ? ` [${e.status}]` : ''}`),
    '',
  ]
  const all = s.errors.map((e) => `${e.message} ${e.status}`).join(' ')
  if (/rate-overlimit|429|too many/i.test(all)) {
    lines.push(' >> O WhatsApp está LIMITANDO os envios. Espere algumas horas e aumente o DELAY_MS.')
  } else if (/401|403|logged ?out|banned|not-authorized/i.test(all)) {
    lines.push(' >> Possível desconexão ou bloqueio da conta. Abra o WhatsApp nos celulares antes de continuar.')
  } else if (/ENOTFOUND|ECONN|ETIMEDOUT|Connection Closed|timed out/i.test(all)) {
    lines.push(' >> Problema de rede/conexão. Confira a internet e se os celulares estão online.')
  }
  lines.push(' O progresso está salvo. Resolva o problema e rode o replay de novo para continuar.')
  lines.push(` Detalhes completos: ${FILE}`)
  lines.push('═'.repeat(70))
  return lines.join('\n')
}
