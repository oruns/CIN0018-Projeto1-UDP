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

Aplicação cliente-servidor em Python para transmissão de arquivos sobre UDP. Na primeira entrega os pacotes ficavam à mercê da rede e um datagrama perdido estragava o arquivo. Esta segunda entrega implementa o RDT 3.0 (Stop-and-Wait) na camada de aplicação, então o arquivo chega inteiro mesmo quando o canal perde ou corrompe pacotes.

O envio e o recebimento foram encapsulados na classe `RDTSocket`, em `rdt.py`. Cliente e servidor chamam `rdt_send` e `rdt_recv` no lugar de `sendto` e `recvfrom`, e toda a fragmentação, numeração e retransmissão acontece lá dentro.

A interface do cliente continua sendo a sessão interativa da primeira entrega, com os comandos `SAVE <arquivo>` e `GET <arquivo>`. O que mudou é que agora cada evento do protocolo aparece no terminal no instante em que acontece, e um resumo fecha cada transferência.

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

Os diretórios `rcvd_files_server/` e `rcvd_files_client/` são criados automaticamente na primeira execução.

## Protocolo e Confiabilidade (RDT 3.0)

Todo datagrama leva um cabeçalho de 4 bytes antes do payload.

```text
[TIPO (1 byte)] + [NÚMERO DE SEQUÊNCIA (1 byte)] + [CHECKSUM (2 bytes)] + [DADOS]
```

| Tipos de Pacote | Descrição |
|---|---|
| `b'D'` (Data) | Pacote contendo dados ou comandos. Requer confirmação. |
| `b'A'` (ACK) | Confirmação do último pacote recebido corretamente. |

O checksum é uma soma de verificação de 16 bits em complemento de um, calculada sobre o tipo, o número de sequência e os dados. É ele que permite ao receptor **detectar** que um pacote chegou corrompido, em vez de supor.

### Máquinas de estado

O código segue as FSMs do rdt3.0. Os estados estão nomeados em `rdt.py`, e cada bloco de código traz um comentário dizendo a qual deles pertence.

| Lado | Estados |
|---|---|
| Transmissor (`rdt_send`) | `Esperar chamada 0 de cima`, `Esperar ACK 0`, `Esperar chamada 1 de cima`, `Esperar ACK 1` |
| Receptor (`_maquina_receptor`) | `Esperar 0 de baixo`, `Esperar 1 de baixo` |

O número de sequência alterna entre 0 e 1 para que uma retransmissão atrasada não seja confundida com um pacote novo. O transmissor manda um pacote de cada vez e só avança depois do ACK correspondente, o que também segura o ritmo do envio no que o receptor dá conta de processar.

### Confirmações e retransmissões

O receptor confirma sempre o último pacote que aceitou. Se o checksum falha, ou se o número de sequência não é o esperado, ele repete o ACK anterior.

Do lado do transmissor, quem retransmite é o temporizador de 50ms, e só ele. Um ACK ilegível ou que confirme o pacote anterior não provoca reação nenhuma, o transmissor segue esperando o prazo acabar. O temporizador é armado no envio e rearmado a cada disparo, então um pacote que chegue no meio do caminho não estica o prazo.

Quando o checksum falha, o número de sequência também não é confiável, já que está dentro da área protegida. Por isso o receptor responde a partir do próprio estado, reenviando o `ACK` do último pacote que aceitou.

O transmissor retransmite enquanto não receber a confirmação, sem limite de tentativas. Um canal mais ruim deixa a transferência mais lenta, mas não faz o protocolo desistir de um pacote.

### Duas máquinas ativas ao mesmo tempo

O transmissor e o receptor são máquinas independentes, então cada lado abre dois sockets, um para cada papel. O socket que espera ACK nunca recebe dados e o que espera dados nunca recebe ACK, de modo que nenhuma das duas máquinas precisa de um ramo a mais para lidar com pacote que não é dela.

A máquina do receptor roda numa thread própria e fica sempre escutando, mesmo enquanto a aplicação está transmitindo. Sem isso o lado que estivesse no meio de um envio deixaria de atender as retransmissões do parceiro, e os dois ficariam retransmitindo sem ninguém do outro lado para responder.

### Simulação de ambiente hostil

O ruído é injetado num único ponto, a função `send_pkt`, que faz o papel do canal. Todo pacote entregue a ela, de dados ou de controle, passa por um sorteio e pode sumir, pode ter um bit invertido antes de seguir viagem, ou pode chegar intacto. Como perda e corrupção saem do mesmo sorteio, elas são mutuamente exclusivas e cada uma respeita a probabilidade configurada.

| Constante | Valor | Descrição |
|---|---|---|
| `LOSS_PROB` | `0.15` (15%) | Probabilidade de o canal descartar o pacote. |
| `CORRUPT_PROB` | `0.15` (15%) | Probabilidade de o canal inverter um bit do pacote, detectado pelo checksum. |
| `TIMEOUT` | `0.05` seg | Tempo de espera do remetente antes da retransmissão. |
| `VERBOSE` | `True` | Imprime cada evento do protocolo em tempo de execução. |

A corrupção pode cair em qualquer byte, cabeçalho incluído. O receptor confere o checksum antes de olhar qualquer outro campo, e quando ele falha a resposta vem do estado local, já que o número de sequência recebido também não é confiável.

## Portas

| Porta | Quem escuta | Quem envia | Tráfego |
|---|---|---|---|
| `6767` | Servidor | Cliente | Comandos `SAVE`/`GET` e os pacotes do arquivo enviado |
| `7676` | Cliente | Servidor | Resposta do servidor e os pacotes do arquivo devolvido |

As duas portas acima recebem os pacotes de dados. Cada lado abre também um socket de transmissão, em porta efêmera, que leva os dados para fora e recolhe os ACKs deles. O ACK volta sempre para a origem do datagrama que o provocou.

## Instruções de Execução

O sistema deve ser testado com dois terminais simultâneos.

### 1. Iniciar o servidor

No primeiro terminal, dentro da pasta do projeto.

```bash
python3 server.py
```

O servidor passa a escutar em `127.0.0.1:6767` e registra tudo o que recebe e envia.

### 2. Abrir a sessão do cliente

No segundo terminal.

```bash
python3 client.py
```

Valem os mesmos comandos da primeira entrega, `SAVE <arquivo>`, `GET <arquivo>` e `SAIR`.

### 3. Acompanhar o protocolo

Cada evento sai no terminal prefixado por `[RDT]`, no instante em que acontece.

```text
[RDT]       pacote 0 íntegro, envia ACK 0
[RDT]       (canal) CORROMPEU ACK seq=0
[RDT]       timeout, retransmite
[RDT]       ACK 1 recebido
```

Do outro lado aparece o receptor recusando o pacote corrompido e aceitando a retransmissão.

```text
[RDT]       checksum inválido, reenvia ACK 1
[RDT]       pacote 0 íntegro, envia ACK 0
```

No fim de cada transferência uma linha consolida o esforço total.

```text
[RDT 3.0] Resumo -> Timeouts: 64 | Retransmissões: 64 | ACKs reenviados: 1 | ACKs ignorados: 11 | Perdas no canal: 25 | Corrupções no canal: 21 | Corrompidos detectados: 13
```

Em arquivos grandes o volume de linhas atrapalha a leitura. Nesses casos basta definir `VERBOSE = False` em `rdt.py`, que o log por evento some e o resumo continua.

### 4. Conferir a integridade

```bash
md5sum src/* rcvd_files_server/* rcvd_files_client/*
```

Os três hashes de um mesmo arquivo devem bater, o que mostra que nenhum byte se perdeu no caminho.

## Testes

A sessão registrada em `log_cliente.txt` e `log_server.txt` cobre três formatos diferentes, com `VERBOSE = True`, e termina com a conferência dos hashes.

| Arquivo | Tipo | Tamanho | Pacotes | Resultado |
|---|---|---|---|---|
| `texto.txt` | texto | 51 B | 1 | Íntegro |
| `foto_teste.jpeg` | imagem | 63.541 B | 63 | Íntegro |
| `teste50k.bin` | binário | 51.200 B | 50 | Íntegro |

A mesma sessão inclui um `GET` de arquivo inexistente, demonstrando o tratamento de erro do servidor.

Para um teste de carga maior há o `src/dado_binario.bin`, com 3.145.728 bytes em 3072 pacotes. Ele leva alguns minutos e vale rodar com `VERBOSE = False`.
