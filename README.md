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

A interface do cliente permanece a mesma: uma sessão interativa aceitando os comandos `SAVE <arquivo>` e `GET <arquivo>`. Contudo, agora toda transferência exibe um Relatório Estatístico do RDT 3.0 ao final, informando quantos erros ocorreram e foram corrigidos de forma autônoma pelo protocolo na rede.

## Estrutura de diretórios

```text
.
├── rdt.py                    # Máquina de estados RDT 3.0 (Timeouts, ACKs, NAKs, Perdas)
├── server.py                 # Servidor UDP (agora utiliza RDTSocket)
├── client.py                 # Cliente UDP (agora utiliza RDTSocket)
├── src/                      # Arquivos disponíveis para envio (entrada do cliente)
├── rcvd_files_server/        # Arquivos recebidos pelo servidor  -> prefixo servidor_
├── rcvd_files_client/        # Arquivos devolvidos ao cliente    -> prefixo cliente_
├── log_server.txt            # Log de execução (terminal do servidor)
└── log_cliente.txt           # Log de execução (terminal do cliente)
```

Os diretórios `rcvd_files_server/` e `rcvd_files_client/` são criados automaticamente na
primeira execução.

## Protocolo e Confiabilidade (RDT 3.0)

Todos os datagramas transferidos pela rede agora possuem um cabeçalho customizado de 2 bytes antes do Payload:
`[TIPO (1 byte)] + [NÚMERO DE SEQUÊNCIA (1 byte)] + [DADOS]`

| Tipos de Pacote | Descrição |
|---|---|
| `b'D'` (Data) | Pacote contendo dados ou comandos. Requer confirmação. |
| `b'A'` (ACK) | Confirmação de recebimento bem-sucedido. |
| `b'N'` (NAK) | Aviso de que o pacote chegou corrompido. |

**Mecanismos de Confiabilidade Implementados:**
* **Numeração de Sequência Alternada (0 e 1):** Impede que pacotes retransmitidos atrasados sejam confundidos com novos dados.
* **Stop-and-Wait:** O remetente envia 1 pacote e aguarda até receber o ACK correspondente antes de enviar o próximo fragmento.
* **Temporizadores (Timeouts):** Se um pacote `D` ou seu `ACK` for perdido, o remetente retransmite automaticamente após 50ms.
* **Detecção de Corrupção e NAKs:** Se o receptor recebe dados corrompidos, solicita o reenvio imediato através de um `NAK`, sem a necessidade de aguardar o Timeout do remetente.

**Simulação de Ambiente Hostil (`rdt.py`):**
Para provar a robustez do algoritmo localmente, inserimos variáveis que simulam um ambiente de rede caótico (injetando ruídos controlados nas funções de *socket*):

| Constante | Valor | Descrição |
|---|---|---|
| `LOSS_PROB` | `0.15` (15%) | Probabilidade de um pacote ou ACK sumir na rede. |
| `CORRUPT_PROB` | `0.15` (15%) | Probabilidade de um pacote chegar com bits trocados (corrompido). |
| `TIMEOUT` | `0.05` seg | Tempo de espera do remetente antes da retransmissão. |

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

**Decisão de Design: O Resumo Estatístico do RDT** 
Durante o desenvolvimento, observamos que imprimir uma mensagem no terminal para cada pacote perdido ou corrompido tornava a aplicação inutilizável para arquivos grandes. Na transferência do binário de 3MB, a simulação agressiva de 15% de falhas gerava um volume tão massivo de logs individuais de `Timeout` e `NAK` que o terminal ficava completamente afogado e ilegível.

Para manter a responsividade e a clareza da interface, projetamos a classe `RDTSocket` para contabilizar esses incidentes silenciosamente na memória. Apenas quando a transferência é finalizada, a aplicação imprime uma única linha agrupando todo o esforço que o protocolo fez nos bastidores:
`[RDT 3.0] Resumo -> Timeouts: X | NAKs enviados: Y | NAKs recebidos: Z`

### 3. Conferir a integridade absoluta

Mesmo com perdas e corrupções extremas simuladas no código, os arquivos chegam idênticos. Para atestar o funcionamento do RDT, execute:
```bash
md5sum src/* rcvd_files_server/* rcvd_files_client/*
```
Os três hashes de um mesmo arquivo serão perfeitamente idênticos, garantindo ausência total de bytes corrompidos e provando a confiabilidade do protocolo sobre um canal ruidoso.

## Testes e Observações Finais

A execução registrada nos logs cobre três tipos de arquivos numa mesma sessão, atestando o suporte a diferentes formatos e tamanhos sem corrupção:

| Arquivo | Tipo | Tamanho | Pacotes | Status da Transmissão |
|---|---|---|---|---|
| `texto.txt` | texto | 51 B | 1 | Sucesso Absoluto |
| `foto_teste.jpeg` | imagem | 63.541 B | 63 | Sucesso Absoluto |
| `dado_binario.bin` | binário | 3.145.728 B | 3072 | Sucesso Absoluto |

O mecanismo de Stop-and-Wait regula naturalmente o tráfego da rede, enviando pacotes ao ritmo exato que o recebedor suporta processar, já que o protocolo exige o `ACK` do datagrama anterior antes de prosseguir. 

Centenas de `Timeouts` e `NAKs` são registrados e ativamente resolvidos pelo algoritmo a cada transferência de arquivos grandes. Contudo, para o usuário final da aplicação (a camada superior do cliente/servidor), a transferência se concretiza de forma completamente transparente e sem erros, comprovando os ensinamentos teóricos da disciplina na prática.
