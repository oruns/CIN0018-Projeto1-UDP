# Projeto 1 - Transmissão de Arquivos com UDP

**Disciplina:** CIN0018 - Fundamentos de Redes de Computadores
**Docente:** Renato Mariz de Moraes
**Grupo:** 2

## Integrantes
* Anderson Vitor Leoncio de Lima
* Edivaldo Ambrozio da Silva Filho
* Guilherme de Carvalho Fabri
* Gustavo Cravo Teixeira Filho
* Titho Lívio Duarte Melo

## Descrição do Projeto

Aplicação cliente-servidor em Python para transmissão de arquivos sobre UDP, usando
diretamente a biblioteca `socket`. Os arquivos são fragmentados em pacotes de até
**1024 bytes** e reconstruídos no destino.

O cliente roda como uma **sessão interativa**: uma vez aberto, ele aceita os comandos
`SAVE <arquivo>` e `GET <arquivo>` quantas vezes o usuário quiser, em qualquer ordem.
`SAVE` envia um arquivo de `src/` para o servidor, que o persiste em disco; `GET`
solicita de volta um arquivo já armazenado, o que permite confirmar que a transmissão
foi bem-sucedida.

A comunicação usa **duas portas fixas**, uma para cada sentido. O cliente envia para a
`6767`, onde o servidor escuta, e o servidor devolve para a `7676`, onde o cliente
escuta.

Conforme o enunciado, os arquivos são armazenados **com um novo nome**. O servidor grava
o que recebe com o prefixo `cliente_`, e o cliente grava o que recebe de volta com o
prefixo `servidor_`. Os prefixos são convenção local de cada lado e eles **não** circulam
no protocolo, que sempre usa o nome original do arquivo.

## Estrutura de diretórios

```
.
├── server.py                 # Servidor UDP
├── client.py                 # Cliente UDP
├── src/                      # Arquivos disponíveis para envio (entrada do cliente)
├── rcvd_files_server/        # Arquivos recebidos pelo servidor  -> prefixo cliente_
├── rcvd_files_client/        # Arquivos devolvidos ao cliente    -> prefixo servidor_
├── log_server.txt            # Log de uma execução completa (terminal do servidor)
└── log_cliente.txt           # Log da mesma execução (terminal do cliente)
```

Os diretórios `rcvd_files_server/` e `rcvd_files_client/` são criados automaticamente na
primeira execução.

## Fluxo de uma transferência

```
src/texto.txt
   ── SAVE "texto.txt" ──▶ :6767    grava rcvd_files_server/cliente_texto.txt
   ── GET  "texto.txt" ──▶ :6767    (resolve internamente para cliente_texto.txt)
   :7676 ◀── conteúdo ────          grava rcvd_files_client/servidor_texto.txt
```

`SAVE` e `GET` são comandos independentes: o `GET` acima confirma a transmissão, mas
pode ser pedido a qualquer momento, para qualquer arquivo já armazenado no servidor.

## Portas

| Porta | Quem escuta | Quem envia | Tráfego |
|---|---|---|---|
| `6767` | Servidor | Cliente | Comandos `SAVE`/`GET` e os pacotes do arquivo enviado |
| `7676` | Cliente | Servidor | Confirmações, erros e os pacotes do arquivo devolvido |

O servidor não responde para a porta de origem do datagrama. Ele monta o destino como
`(ip_de_origem, CLIENT_PORT)`, de modo que o sentido servidor para cliente sempre usa
a `7676`. Como a porta do cliente é fixa, **apenas um cliente pode executar por vez**
na mesma máquina.

## Protocolo

Cada comando é um datagrama próprio.

| Comando | Formato | Resposta |
|---|---|---|
| `SAVE` | `b"SAVE"` + tamanho (32 bytes, big-endian) + `nome.extensão`, seguido de `ceil(tamanho / 1024)` datagramas com o conteúdo | Uma linha de texto com a confirmação ou o erro |
| `GET` | `b"GET "` + `nome.extensão` (o nome original, sem prefixo) | Uma linha de texto com o tamanho em bytes, seguida de `ceil(tamanho / 1024)` datagramas com o conteúdo. Em caso de erro, uma linha de texto com o erro |

**Parâmetros de configuração** (constantes no topo de cada arquivo):

| Constante | Valor | Descrição |
|---|---|---|
| `SERVER_IP` / `LOCAL_HOST_IPV4` | `127.0.0.1` | Endereço do servidor |
| `SERVER_PORT` / `ENTRY_PORT` | `6767` | Porta em que o servidor escuta |
| `CLIENT_IP` | `127.0.0.1` | Endereço do cliente |
| `CLIENT_PORT` | `7676` | Porta em que o cliente escuta |
| `BUFFER_SIZE` | `1024` bytes | Tamanho máximo de cada datagrama |
| `CHUNK_SIZE` | `65536` bytes | Acumulado em memória antes de cada gravação em disco |
| `DATA_SIZE_LIM` | `134217728` bytes (128 MB) | Tamanho máximo aceito por arquivo |

## Pré-requisitos

* Python 3 instalado no sistema (nenhuma dependência externa).
* Uma interface de linha de comandos (Terminal/Prompt).

## Instruções de Execução

O sistema deve ser testado com **dois terminais simultâneos**.

### 1. Iniciar o servidor

No primeiro terminal, na pasta do projeto:

```bash
python3 server.py
```

O servidor passa a escutar em `127.0.0.1:6767` e registra no terminal tudo o que recebe
e envia:

```
[SERVIDOR]  Escutando em 127.0.0.1:6767 - respondendo na porta 7676
[SERVIDOR]  Armazenando em ./rcvd_files_server/
```

### 2. Abrir a sessão do cliente

Coloque os arquivos a transmitir em `src/` e, no segundo terminal:

```bash
python3 client.py
```

O cliente informa o par de portas em uso e abre a sessão:

```
[CLIENTE] Enviando para 127.0.0.1:6767 - escutando em 127.0.0.1:7676
============================================================
Sessão iniciada. Comandos disponíveis:
  SAVE <nome_do_arquivo>  (Envia um arquivo da pasta ./src/)
  GET <nome_do_arquivo>   (Solicita um arquivo do servidor)
  SAIR                    (Encerra o cliente)
============================================================
```

| Comando | O que faz |
|---|---|
| `SAVE <arquivo>` | Envia `src/<arquivo>` ao servidor, que grava em `rcvd_files_server/cliente_<arquivo>` |
| `GET <arquivo>` | Pede ao servidor o arquivo `<arquivo>`, gravando a devolução em `rcvd_files_client/servidor_<arquivo>` |
| `SAIR` | Encerra a sessão (`EXIT` e `Ctrl+C` também funcionam) |

Os comandos podem ser dados na ordem que o usuário quiser e quantas vezes quiser, sem
reiniciar o cliente. Uma sessão de exemplo:

```
UDP> SAVE texto.txt
[ENVIANDO] Iniciando envio de 'texto.txt' (51 bytes em 1 pacotes)
[SERVIDOR] Arquivo texto.txt salvo.

UDP> GET texto.txt
[SOLICITANDO] Requisitando 'texto.txt' do servidor...
[RECEBENDO] Arquivo terá 51 bytes. Aguardando 1 pacotes...
[CONCLUÍDO] Arquivo recuperado com sucesso e salvo como './rcvd_files_client/servidor_texto.txt'
```

A sessão completa registrada em `log_cliente.txt` inclui também um `GET` de arquivo
inexistente, para demonstrar o tratamento de erro do servidor.

### 3. Conferir a integridade

```bash
md5sum src/* rcvd_files_server/* rcvd_files_client/*
```

Os três hashes de um mesmo arquivo devem ser idênticos.

## Funcionalidades obrigatórias

| Requisito | Onde está implementado |
|---|---|
| Envio de arquivos | Comando `SAVE`, função `send_file()` em `client.py` |
| Armazenamento | Função `store()` em `server.py`, que persiste em `rcvd_files_server/` com o prefixo `cliente_` |
| Devolução de arquivos | Comando `GET`, funções `send()` em `server.py` e `get_file()` em `client.py` |
| Suporte a múltiplos tipos | Testado com `.txt`, `.jpeg` e binário `.bin` (tabela abaixo) |
| Interface do usuário | O cliente recebe o nome do arquivo como parâmetro dos comandos `SAVE`/`GET`, e o servidor registra no terminal cada arquivo que recebe e envia (ver `log_server.txt`) |

## Testes realizados

A execução registrada em `log_server.txt` e `log_cliente.txt` cobre três tipos de arquivo
numa mesma sessão, todos com os hashes conferidos ao final:

| Arquivo | Tipo | Tamanho | Pacotes |
|---|---|---|---|
| `texto.txt` | texto | 51 B | 1 |
| `foto_teste.jpeg` | imagem | 63.541 B | 63 |
| `dado_binario.bin` | binário | 3.145.728 B | 3072 |

## Observações

Conforme o enunciado, **não** foram implementados mecanismos de transferência confiável
(ACKs, NAKs ou retransmissões), já que o foco desta etapa é o funcionamento básico do UDP.
Como consequência esperada do protocolo, transferências de arquivos grandes podem perder
datagramas quando o buffer de recepção do socket satura (o que ocorre ocasionalmente com
o `dado_binario.bin` de 3 MB).
