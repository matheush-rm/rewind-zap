# rewind-zap

Recria uma conversa de WhatsApp entre **A** (tem o histórico) e **B** (perdeu), reenviando cada
mensagem pelo seu remetente original, na ordem cronológica. A copia nova que surge no celular de A
é apagada "para mim" logo em seguida, para não duplicar.

Usa a lib [Baileys](https://github.com/WhiskeySockets/Baileys) direto (a Evolution não tem "apagar para mim").

## Antes de começar
- Celulares de A e B **ligados e online** durante todo o processo.
- **Não conversem entre si** durante o export e o replay.
- Silencie a conversa nos dois celulares e desative o download automático de mídia em A
  (senão a galeria de A ganha cópias das mídias que B reenviar).
- Volume alto de mensagens pode chamar atenção do anti-spam. Não abaixe muito o `DELAY_MS`.

## Passo a passo
```sh
cp .env.example .env              # edite PARTNER = número de B
docker compose build

# 1) Exporta: pareie o QR com o celular de A. Captura o histórico, baixa as mídias, gera data/plan.json
docker compose run --rm app export

# 2) Confira a transcrição (ordem, remetentes, mídias)
docker compose run --rm app preview
docker compose run --rm app preview --skipped

# 3) Teste com poucas mensagens: pareie o QR com o celular de B; confira os dois celulares
docker compose run --rm app replay --max 3

# 4) Roda tudo (retoma exatamente de onde parou; reinicia sozinho se cair)
docker compose run --rm app replay

# Em outro terminal, a qualquer momento: progresso sem interromper nada
docker compose run --rm app status
```

## Progresso e erros
- Durante o replay, a última linha do terminal mostra o contador: enviadas/total, %, A→B e B→A,
  enviadas nesta sessão, velocidade, tempo restante, exclusões pendentes e erros seguidos.
  Sem terminal interativo (logs), a mesma linha é impressa a cada 100 mensagens.
- **Circuit breaker**: após 10 erros seguidos (`BREAKER_LIMIT`) de envio ou de exclusão, o replay
  para e mostra o último erro (mensagem, status, onde ocorreu) e os anteriores, com uma dica quando o
  erro indica limite do WhatsApp, conta desconectada ou rede. Quedas de conexão também contam, mesmo
  entre os reinícios automáticos. Qualquer mensagem concluída zera a contagem. Ao rodar o replay de
  novo manualmente, a contagem recomeça do zero. O histórico fica em `data/breaker.json`.
Sem Docker: `npm ci`, depois `PARTNER=55... sh run.sh export|preview|replay`.

## Volume alto (dezenas de milhares de mensagens)
- **Tempo**: cada mensagem espera a anterior ser recebida, então o ritmo fica em torno de
  0,5–1 msg/s (texto). 75 mil mensagens ≈ **1 a 2 dias** rodando, mais o upload das mídias.
  O replay mostra a cada 100 mensagens a velocidade real e o tempo restante.
- **Pode parar e continuar** quando quiser (Ctrl+C, `--max N`, queda de conexão). Rodar em blocos
  (ex: `--max 5000` por sessão) também reduz o risco de o anti-spam do WhatsApp agir.
- **Mídia**: o export baixa em paralelo (`MEDIA_CONCURRENCY`) e é retomável. Reserve espaço em
  disco para todas as mídias da conversa. Mídia antiga é pedida de novo ao celular de A; o que
  não existir mais lá aparece como falha no relatório.
- **Exclusões em A**: rodam em uma fila separada (`DELETE_DELAY_MS`), sem atrasar os envios.
  Alternativa para volume muito alto: faça um backup do WhatsApp de A antes, rode com `NO_DELETE=1`
  e no fim restaure o backup em A (reinstalando o app). Isso elimina dezenas de milhares de exclusões.

## Garantias
- **Ordem**: cada mensagem só é enviada depois que a anterior foi *recebida* pela outra conta (e com pelo menos `DELAY_MS` de intervalo). As exclusões em A não interferem na ordem.
- **Sem vazamento**: o replay valida que a sessão A é o dono do plano e a sessão B é o contato do plano; a única
  conversa usada é A↔B. Mensagens de outras conversas nem são gravadas no export.
- **Retomada exata**: `data/state.json` guarda o passo de cada mensagem (enviar → recebida) e a fila de exclusões pendentes em A.
  O ID de cada mensagem é determinístico, então um reenvio após queda não duplica.
- O histórico completo só é enviado pelo WhatsApp **no primeiro pareamento** de A. Se o export falhar no
  meio dessa sincronização, desconecte o aparelho no celular de A, apague `data/auth/a` e rode de novo.
- Depois que o replay começa, o plano fica congelado (o export se recusa a rodar).

## O que é recriado
Texto, imagem, vídeo, GIF, vídeo redondo, áudio/nota de voz, figurinha (inclusive animada), documento,
localização, contato, enquete (sem os votos), respostas/citações, reações e edições (texto final).
Mensagens apagadas para todos e avisos de sistema são ignorados (`preview --skipped`).

## Recomeçar do zero
Apague `data/` (e desconecte os aparelhos "Mac OS" nos dois celulares).
