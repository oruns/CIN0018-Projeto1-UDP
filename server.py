"""
Recebe arquivos do cliente, armazena em STORAGE_DIR com o prefixo "servidor_"
e devolve o arquivo armazenado quando solicitado. O prefixo é uma convenção
interna de armazenamento e não aparece no protocolo: o cliente usa o nome
original do arquivo tanto no SAVE quanto no GET. Os arquivos são fragmentados
em pacotes de até 1024 bytes.

A comunicação usa duas portas: ENTRY_PORT e CLIENT_PORT. O cliente envia para ENTRY_PORT,
onde o servidor escuta, e o servidor devolve para CLIENT_PORT, onde o cliente escuta.

Protocolo (cada comando é um datagrama próprio):

    SAVE -> b"SAVE" + tamanho (32 bytes, big-endian) + nome.extensão
            seguido de ceil(tamanho / 1024) datagramas com o conteúdo.
            Resposta: uma linha de texto com a confirmação ou o erro.

    GET  -> b"GET " + nome.extensão (o nome original, sem o prefixo)
            Resposta: uma linha de texto com o tamanho do arquivo em bytes,
            seguida de ceil(tamanho / 1024) datagramas com o conteúdo. Em
            caso de erro, retorna uma linha de texto com o erro.

"""

import socket
import os


LOCAL_HOST_IPV4 =   "127.0.0.1"
ENTRY_PORT =        6767      # o servidor escuta aqui (cliente -> servidor)
CLIENT_PORT =       7676      # o cliente escuta ali   (servidor -> cliente)

DATA_SIZE_LIM   =   2**27     # 128 MB. Tamanho máximo aceito por arquivo
CHUNK_SIZE      =   2**16     # 64 KB.  Acumulado em memória antes de gravar
BUFFER_SIZE     =   2**10     # 1  KB.  Tamanho máximo de cada datagrama

STORAGE_DIR     =   "./rcvd_files_server/"
PREFIX          =   "servidor_"  # só no disco; o protocolo usa o nome original


def valid_name(name_and_extension):
    """Verifica se o nome esta no formato nome.extensao.

    rpartition divide no último ponto, então uma string do formato "semponto"
    deixa o separador vazio, ".jpeg" deixa o nome vazio e "foto." deixa a extensão vazia.
    A barra é recusada para que o arquivo não escape de STORAGE_DIR.
    """
    name, dot, extension = name_and_extension.rpartition(".")
    return bool(name and dot and extension) and "/" not in name_and_extension


def store(n_packets, name_and_extension, server):
    """Recebe n_packets datagramas e grava o arquivo em STORAGE_DIR com o PREFIX.

    O conteúdo é acumulado em memória e so vai para o disco a cada CHUNK_SIZE
    (ou no último pacote), para não fazer uma escrita por datagrama.
    """
    buffer = bytearray()
    path = f"{STORAGE_DIR}{PREFIX}{name_and_extension}"

    print(f"[RECEBENDO] {name_and_extension} -> {path} ({n_packets} pacotes esperados)")

    # O arquivo é aberto uma única vez em modo "wb"
    with open(path, "wb") as f:
        for packet in range(n_packets):
            data, _ = server.recvfrom(BUFFER_SIZE)
            buffer.extend(data)
            if (len(buffer) >= CHUNK_SIZE) or (packet == n_packets - 1):
                f.write(buffer)
                print(f"[RECEBENDO] pacote {packet + 1}/{n_packets} - {len(buffer)} bytes gravados em disco")
                buffer.clear()

    print(f"[RECEBIDO]  {path} - {os.path.getsize(path)} bytes")


def send(name_and_extension, addr, server):
    """Envia o tamanho do arquivo e, em seguida, o conteúdo fragmentado.

    Recebe o nome original e reconstrói o nome de disco com o PREFIX.
    O addr já aponta para a porta de escuta do cliente (CLIENT_PORT).
    """
    path = f"{STORAGE_DIR}{PREFIX}{name_and_extension}"

    with open(path, "rb") as f:
        file_size = f.seek(0, 2)              # seek até o fim devolve o tamanho
        server.sendto(f"{file_size}".encode(), addr)

        n_packets = (file_size + BUFFER_SIZE - 1) // BUFFER_SIZE

        f.seek(0)                             # o seek acima deixou o ponteiro no fim

        print(f"[ENVIANDO]  {path} - {file_size} bytes em {n_packets} pacotes para {addr}")

        for packet in range(n_packets):
            chunk = f.read(BUFFER_SIZE)
            server.sendto(chunk, addr)

    print(f"[ENVIADO]   {path} para {addr}")


os.makedirs(STORAGE_DIR, exist_ok=True)

server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server.bind((LOCAL_HOST_IPV4, ENTRY_PORT))

print(f"[SERVIDOR]  Escutando em {LOCAL_HOST_IPV4}:{ENTRY_PORT} - respondendo na porta {CLIENT_PORT}")
print(f"[SERVIDOR]  Armazenando em {STORAGE_DIR}")

while True:
    data, addr = server.recvfrom(BUFFER_SIZE)

    # O datagrama chega da porta de origem do cliente, mas toda resposta vai para
    # a porta fixa em que ele escuta (os dois sentidos usam portas distintas).
    client_addr = (addr[0], CLIENT_PORT)

    # Um pedido malformado (comando que não é UTF-8, arquivo sem permissão de
    # leitura, disco cheio) não derruba o servidor. O erro é registrado,
    # o cliente é avisado e o laço segue para o próximo pedido.
    try:
        match data[:4].decode("utf-8").strip():
            case "SAVE":
                file_size = int.from_bytes(data[4:36], "big")
                file_name_and_extension = data[36:].decode("utf-8")

                print(f"[COMANDO]   SAVE de {addr} - {file_name_and_extension} ({file_size} bytes)")

                if not valid_name(file_name_and_extension):
                    print(f"[ERRO]      Nome invalido: {file_name_and_extension!r} - esperado nome.extensao")
                    server.sendto("ERRO: Nome de arquivo inválido.".encode(), client_addr)
                elif file_size > DATA_SIZE_LIM:
                    print(f"[ERRO]      Arquivo maior que o limite de {DATA_SIZE_LIM} bytes")
                    server.sendto("ERRO: Arquivo muito grande.".encode(), client_addr)
                else:
                    n_packets = (file_size +  BUFFER_SIZE - 1) // BUFFER_SIZE
                    store(n_packets, file_name_and_extension, server)
                    server.sendto(f"Arquivo {file_name_and_extension} salvo.".encode(), client_addr)

            case "GET":
                file_name_and_extension = data[4:].decode("utf-8")

                print(f"[COMANDO]   GET de {addr} - {file_name_and_extension}")

                if not valid_name(file_name_and_extension):
                    print(f"[ERRO]      Nome invalido: {file_name_and_extension!r} - esperado nome.extensao")
                    server.sendto("ERRO: Nome de arquivo inválido.".encode(), client_addr)
                    continue

                # O pedido chega com o nome original; em disco ele tem o prefixo
                stored_name = f"{PREFIX}{file_name_and_extension}"

                flag = False
                with os.scandir(STORAGE_DIR) as entries:
                    for entry in entries:
                        if entry.is_file() and entry.name == stored_name:
                            flag = True
                            send(file_name_and_extension, client_addr, server)

                if(not flag):
                    print(f"[ERRO]      Arquivo {stored_name} nao encontrado em {STORAGE_DIR}")
                    server.sendto(f"ERRO: Arquivo não encontrado.".encode(), client_addr)

            case _:
                print(f"[ERRO]      Comando desconhecido de {addr}: {data[:4]}")
                server.sendto("ERRO: Comando desconhecido.".encode(), client_addr)

    except Exception as erro:
        print(f"[ERRO]      Falha ao atender {addr}: {type(erro).__name__}: {erro}")
        server.sendto(f"ERRO: Falha ao processar o pedido ({type(erro).__name__}).".encode(), client_addr)
