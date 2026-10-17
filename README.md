# Projeto 2 - Transmissão Confiável de Arquivos com RDT 3.0 sobre UDP

**Disciplina:** CIN0018 - Fundamentos de Redes de Computadores

**Docente:** Renato Mariz de Moraes

**Grupo:** 2

## Integrantes
* Anderson Vitor Leoncio de Lima
* Edivaldo Ambrozio da Silva Filho
* Guilherme de Carvalho Fabri
* Gustavo Cravo Teixeira Filho
* Titho Lívio Duarte Melo

## Descrição do Projeto (Segunda Entrega)

Aplicação cliente-servidor em Python para transmissão de arquivos sobre UDP. Diferente da primeira entrega, onde os pacotes estavam sujeitos à perda da rede, esta segunda entrega implementa o protocolo de transferência confiável **RDT 3.0 (Stop-and-Wait)** na camada de aplicação.

O protocolo utiliza internamente a biblioteca `socket` do Python, mas as chamadas de envio e recebimento foram encapsuladas em uma classe `RDTSocket`. Essa abstração garante que arquivos, não importa o tamanho, sejam fragmentados, numerados e reconstruídos com 100% de integridade no destino, lidando ativamente com perda de pacotes, perda de confirmações e corrupção de dados.

A interface do cliente permanece a mesma: uma sessão interativa aceitando os comandos `SAVE <arquivo>` e `GET <arquivo>`. Cada evento do protocolo (perda, corrupção, timeout, retransmissão, ACK duplicado) é registrado no terminal no instante em que acontece, e ao final de cada transferência um resumo consolida tudo o que o protocolo resolveu sozinho.

## Estrutura de diretórios

```text
.
├── rdt.py                    # Canal RDT 3.0 (checksum, ACKs, timeouts, perdas)
├── server.py                 # Servidor UDP (agora utiliza RDTSocket)
├── client.py                 # Cliente UDP (agora utiliza RDTSocket)
├── src/                      # Arquivos disponíveis para envio (entrada do cliente)
├── rcvd_files_server/        # Arquivos recebidos pelo servidor  -> prefixo servidor_
├── rcvd_files_client/        # Arquivos devolvidos ao cliente    -> prefixo cliente_
├── log_server.txt            # Log de execução (terminal do servidor)
├── log_cliente.txt           # Log de execução (terminal do cliente)
└── .gitignore                # Ignora bytecode e os binários grandes regerados
```

Os diretórios `rcvd_files_server/` e `rcvd_files_client/` são criados automaticamente na
primeira execução.

## Protocolo e Confiabilidade (RDT 3.0)

Todos os datagramas transferidos pela rede possuem um cabeçalho customizado de 4 bytes antes do payload:
`[TIPO (1 byte)] + [NÚMERO DE SEQUÊNCIA (1 byte)] + [CHECKSUM (2 bytes)] + [DADOS]`

O checksum é uma soma de verificação de 16 bits em complemento de um, calculada sobre o tipo, o número de sequência e os dados. É ele que permite ao receptor **detectar** que um pacote chegou corrompido, em vez de supor.

| Tipos de Pacote | Descrição |
|---|---|
| `b'D'` (Data) | Pacote contendo dados ou comandos. Requer confirmação. |
| `b'A'` (ACK) | Confirmação do último pacote recebido corretamente. |

**Máquinas de estado:**

O código segue as FSMs do rdt3.0, com os estados nomeados em `rdt.py` e registrados no log a cada transição.

| Lado | Estados |
|---|---|
| Transmissor (`rdt_send`) | `Espera chamada 0 de cima`, `Espera por ACK0`, `Espera chamada 1 de cima`, `Espera por ACK1` |
| Receptor (`rdt_recv`) | `Esperar 0 de baixo`, `Esperar 1 de baixo` |

As transições aparecem no terminal na mesma notação do diagrama:

```text
[RDT]       [Espera chamada 0 de cima] rdt_send(data) -> udt_send(sndpkt), start_timer
[RDT]       [Espera por ACK0] rdt_rcv && corrupt(rcvpkt) -> Λ
[RDT]       [Espera por ACK0] timeout -> udt_send(sndpkt), start_timer
[RDT]       [Espera por ACK0] rdt_rcv && notcorrupt && isACK(0) -> stop_timer => Espera chamada 1 de cima
```

```text
[RDT]       [Esperar 0 de baixo] rdt_rcv && corrupt(rcvpkt) -> udt_send(ACK1)
[RDT]       [Esperar 0 de baixo] rdt_rcv && notcorrupt && has_seq0 -> extract, deliver_data, udt_send(ACK0) => Esperar 1 de baixo
```

**Mecanismos de Confiabilidade Implementados:**
* **Numeração de Sequência Alternada (0 e 1):** Impede que pacotes retransmitidos atrasados sejam confundidos com novos dados.
* **Stop-and-Wait:** O remetente envia 1 pacote e aguarda até receber o ACK correspondente antes de enviar o próximo fragmento.
* **Temporizadores (Timeouts):** Se um pacote `D` ou seu `ACK` for perdido, o remetente retransmite automaticamente após 50ms.
* **Checksum e ACK duplicado:** O receptor recalcula o checksum de todo pacote que chega. Se o valor não confere, ou se o pacote não é o esperado, ele reenvia o `ACK` do último pacote aceito, e o remetente retransmite na hora, sem esperar o temporizador estourar.
* **Retransmissão sem desistência:** O remetente retransmite enquanto não receber o `ACK` correspondente. O protocolo nunca descarta um dado, de modo que a entrega independe da taxa de perda do canal.

Quando o checksum falha, o número de sequência também não é confiável, já que está dentro da área protegida. Por isso o receptor responde a partir do próprio estado, reenviando o `ACK` do último pacote que aceitou.

**Simulação de Ambiente Hostil (`rdt.py`):**
O ruído é injetado em um único ponto, a função `send_pkt`, que representa o canal. Todo pacote entregue a ela (dados ou ACK) passa por um sorteio: pode sumir, pode ter um bit invertido antes de seguir viagem, ou pode chegar intacto. Como perda e corrupção saem do mesmo sorteio, elas são mutuamente exclusivas e cada uma respeita exatamente a probabilidade configurada.

| Constante | Valor | Descrição |
|---|---|---|
| `LOSS_PROB` | `0.15` (15%) | Probabilidade de o canal descartar o pacote. |
| `CORRUPT_PROB` | `0.15` (15%) | Probabilidade de o canal inverter um bit do pacote, detectado pelo checksum. |
| `TIMEOUT` | `0.05` seg | Tempo de espera do remetente antes da retransmissão. |
| `VERBOSE` | `True` | Imprime cada evento do protocolo em tempo de execução. |

A corrupção atinge o número de sequência, o checksum e o payload, preservando apenas o byte de tipo. O receptor não precisa confiar no número de sequência de um pacote corrompido, porque a resposta vem do estado local.

## Portas

| Porta | Quem escuta | Quem envia | Tráfego |
|---|---|---|---|
| `6767` | Servidor | Cliente | Comandos `SAVE`/`GET` e os pacotes do arquivo enviado |
| `7676` | Cliente | Servidor | Confirmações, erros e os pacotes do arquivo devolvido |

## Instruções de Execução

O sistema deve ser testado com dois terminais simultâneos.

### 1. Iniciar o servidor

No primeiro terminal, na pasta do projeto:
```bash
python3 server.py
```

O servidor passa a escutar em `127.0.0.1:6767` e registra no terminal tudo o que recebe
e envia.

### 2. Abrir a sessão do cliente

No segundo terminal:
```bash
python3 client.py
```

Você pode usar os mesmos comandos da primeira entrega:
* `SAVE <arquivo>`
* `GET <arquivo>`
* `SAIR`

**Transparência: log em tempo real e resumo consolidado**

Cada evento do protocolo é impresso no terminal no instante em que ocorre, prefixado por `[RDT]`, de modo que dá para acompanhar a máquina de estados funcionando:

```text
[RDT]       (canal) CORROMPEU dados seq=0
[RDT]       [Espera por ACK0] rdt_rcv && isACK(1) -> udt_send(sndpkt), start_timer
[RDT]       (canal) PERDEU dados seq=0
[RDT]       [Espera por ACK0] timeout -> udt_send(sndpkt), start_timer
```

Ao final de cada transferência, uma linha consolida o esforço total:

```text
[RDT 3.0] Resumo -> Timeouts: 66 | Retransmissões: 74 | ACKs reenviados: 0 | ACKs duplicados recebidos: 8 | Perdas no canal: 35 | Corrupções no canal: 18 | Corrompidos detectados: 20
```

Em transferências muito grandes o volume de linhas individuais dificulta a leitura. Para esses casos, basta definir `VERBOSE = False` em `rdt.py`: o log por evento é suprimido e o resumo continua sendo impresso.

### 3. Conferir a integridade absoluta

Mesmo com perdas e corrupções extremas simuladas no código, os arquivos chegam idênticos. Para atestar o funcionamento do RDT, execute:
```bash
md5sum src/* rcvd_files_server/* rcvd_files_client/*
```
Os três hashes de um mesmo arquivo serão perfeitamente idênticos, garantindo ausência total de bytes corrompidos e provando a confiabilidade do protocolo sobre um canal ruidoso.

## Testes e Observações Finais

A sessão registrada em `log_cliente.txt` e `log_server.txt` cobre três formatos diferentes, com `VERBOSE = True`, e termina com a conferência dos hashes:

| Arquivo | Tipo | Tamanho | Pacotes | Resultado |
|---|---|---|---|---|
| `texto.txt` | texto | 51 B | 1 | Íntegro |
| `foto_teste.jpeg` | imagem | 63.541 B | 63 | Íntegro |
| `teste50k.bin` | binário | 51.200 B | 50 | Íntegro |

A mesma sessão inclui um `GET` de arquivo inexistente, demonstrando o tratamento de erro do servidor.

O arquivo `src/dado_binario.bin` (3.145.728 B, 3072 pacotes) foi usado como teste de carga à parte, com `VERBOSE = False` para não afogar o terminal. A transferência completa levou cerca de 4 minutos e chegou íntegra nas duas pontas, tendo atravessado 877 perdas e 882 corrupções de canal no envio e outras 733 perdas e 692 corrupções na devolução.

O mecanismo de Stop-and-Wait regula naturalmente o tráfego da rede, enviando pacotes ao ritmo exato que o recebedor suporta processar, já que o protocolo exige o `ACK` do datagrama anterior antes de prosseguir. 

Centenas de `Timeouts` e retransmissões são registrados e ativamente resolvidos pelo algoritmo a cada transferência de arquivos grandes. Contudo, para o usuário final da aplicação (a camada superior do cliente/servidor), a transferência se concretiza de forma completamente transparente e sem erros, comprovando os ensinamentos teóricos da disciplina na prática.

Como o remetente retransmite indefinidamente, a confiabilidade não depende de quão ruim é o canal: aumentar `LOSS_PROB` e `CORRUPT_PROB` apenas torna a transferência mais lenta. Em testes com 40% de perda e 40% de corrupção simultâneas, os arquivos continuaram chegando sem um único byte trocado.
