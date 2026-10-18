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

Os dois tipos de pacote seguem o formato que o livro escreve, `make_pkt(seq, data, checksum)` para os dados e `make_pkt(ACK, seq, checksum)` para a confirmação.

```text
dados        [NÚMERO DE SEQUÊNCIA (1 byte)] + [CHECKSUM (2 bytes)] + [DADOS até 1024 bytes]
confirmação  [ACK (1 byte)] + [NÚMERO DE SEQUÊNCIA (1 byte)] + [CHECKSUM (2 bytes)]
```

O pacote de dados não precisa de campo dizendo o que é, porque o canal por onde ele chega já define isso.

O checksum é uma soma de verificação de 16 bits em complemento de um, calculada sobre tudo o que vem antes dele no pacote. É ele que permite **detectar** que um pacote chegou corrompido, em vez de supor.

### Máquinas de estado

O código segue as FSMs do rdt3.0. Os estados estão nomeados em `rdt.py`, e cada bloco de código traz um comentário dizendo a qual deles pertence.

| Lado | Estados |
|---|---|
| Transmissor (`rdt_send`) | `Esperar chamada 0 de cima`, `Esperar ACK 0`, `Esperar chamada 1 de cima`, `Esperar ACK 1` |
| Receptor (`_receptor`) | `Esperar 0 de baixo`, `Esperar 1 de baixo` |

O número de sequência alterna entre 0 e 1 para que uma retransmissão atrasada não seja confundida com um pacote novo. O transmissor manda um pacote de cada vez e só avança depois do ACK correspondente, o que também segura o ritmo do envio no que o receptor dá conta de processar.

### Confirmações e retransmissões

O receptor confirma sempre o último pacote que aceitou. Se o checksum falha, ou se o número de sequência não é o esperado, ele repete o ACK anterior.

Do lado do transmissor, quem retransmite é o temporizador de 50ms, e só ele. Um ACK ilegível ou que confirme o pacote anterior não provoca reação nenhuma, o transmissor segue esperando o prazo acabar. O temporizador é armado no envio e rearmado a cada disparo, então um pacote que chegue no meio do caminho não estica o prazo.

Quando o checksum falha, o número de sequência também não é confiável, já que está dentro da área protegida. Por isso o receptor responde a partir do próprio estado, reenviando o `ACK` do último pacote que aceitou.

O transmissor retransmite enquanto não receber a confirmação, sem limite de tentativas. Um canal mais ruim deixa a transferência mais lenta, mas não faz o protocolo desistir de um pacote.

### Os dois canais

O transmissor e o receptor são máquinas separadas e cada uma tem o seu canal, como no modelo. O canal de dados leva os pacotes do transmissor de um lado até o receptor do outro, e o canal de confirmação traz os ACKs de volta. Cada lado abre um socket por canal, então o pacote de dados não precisa de campo dizendo o que é, o canal por onde chegou já determina a qual máquina pertence.

As figuras do livro descrevem transferência num sentido só. Aqui cliente e servidor fazem os dois papéis, porque o `SAVE` manda dados e o `GET` recebe dados, e isso cria um problema. Se o lado que acabou de receber virar transmissor e parar de olhar o canal de dados, ele deixa de atender as retransmissões do parceiro, e os dois acabam retransmitindo sem ninguém do outro lado para responder.

Por isso a espera é feita com `select` nos dois canais ao mesmo tempo, tanto no `rdt_send` quanto no `rdt_recv`. O que chega pelo canal de confirmação vai para a máquina do transmissor, o que chega pelo canal de dados vai para a do receptor, e o lado que está transmitindo continua atendendo o que lhe mandam.

### Simulação de ambiente hostil

O ruído é injetado num único ponto, a função `send_pkt`, que faz o papel do canal. Todo pacote entregue a ela, de dados ou de controle, passa por um sorteio e pode sumir, pode ter um bit invertido antes de seguir viagem, ou pode chegar intacto. Como perda e corrupção saem do mesmo sorteio, elas são mutuamente exclusivas e cada uma respeita a probabilidade configurada.

| Constante | Valor | Descrição |
|---|---|---|
| `LOSS_PROB` | `0.15` (15%) | Probabilidade de o canal descartar o pacote. |
| `CORRUPT_PROB` | `0.15` (15%) | Probabilidade de o canal inverter um bit do pacote, que o destino detecta. |
| `TIMEOUT` | `0.05` seg | Tempo de espera do remetente antes da retransmissão. |
| `VERBOSE` | `True` | Imprime cada evento do protocolo em tempo de execução. |

A corrupção pode cair em qualquer byte, cabeçalho incluído. O receptor confere o checksum antes de olhar qualquer outro campo, e quando ele falha a resposta vem do estado local, já que o número de sequência recebido também não é confiável. Numa confirmação, a corrupção pode atingir o identificador, e aí ela é descartada por não trazer mais o `ACK`, que dá no mesmo que falhar no checksum.

## Portas

| Porta | Quem escuta | Quem envia | Tráfego |
|---|---|---|---|
| `6767` | Servidor | Cliente | Comandos `SAVE`/`GET` e os pacotes do arquivo enviado |
| `7676` | Cliente | Servidor | Resposta do servidor e os pacotes do arquivo devolvido |

As duas portas acima são o canal de dados de cada lado. O canal de confirmação usa um socket em porta efêmera, e o ACK volta sempre para a origem do datagrama que o provocou.

### O que difere do modelo do livro

As figuras do Kurose descrevem a transferência num sentido só, entre duas máquinas que não fazem mais nada. Três pontos do projeto não cabem nesse desenho e vale deixá-los declarados.

A perda e a corrupção moram no emissor, dentro da `send_pkt`, e não num canal de verdade. Como não existe uma rede ruim entre dois processos na mesma máquina, é o emissor que sorteia e às vezes deixa de enviar, que é o gerador aleatório de perdas que o enunciado pede.

A entrega para a camada de cima passa por uma fila. No livro o `deliver_data(data)` entrega e acabou, porque a camada de cima está sempre pronta. Aqui o dado pode chegar enquanto a aplicação ainda está dentro de um `rdt_send`, então ele espera até ela chamar `rdt_recv`. Medindo uma sessão inteira, a fila chegou a guardar dois pacotes.

As chamadas carregam o endereço do parceiro. O livro tem `rdt_send(data)` e `deliver_data(data)`, enquanto aqui são `rdt_send(data, addr)` e um retorno com o endereço de origem, porque em UDP é preciso dizer para quem se envia e saber de quem veio.

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
[RDT]       (canal) CORROMPEU dados seq=0
[RDT]       ACK corrompido, ignora
[RDT]       timeout, retransmite
[RDT]       ACK 0 recebido
```

Do outro lado aparece o receptor recusando o pacote corrompido e aceitando a retransmissão.

```text
[RDT]       pacote ilegível, reenvia ACK 0
[RDT]       pacote 1 íntegro, envia ACK 1
```

No fim de cada transferência uma linha consolida o esforço total.

```text
[RDT 3.0] Resumo -> Timeouts: 67 | Retransmissões: 67 | ACKs reenviados: 0 | ACKs ignorados: 12 | Perdas no canal: 27 | Corrupções no canal: 21 | Corrompidos detectados: 18
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
