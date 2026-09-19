import socket
import os
import sys
import math

# Configurações de rede compatíveis com o servidor
SERVER_IP = "127.0.0.1"
SERVER_PORT = 6767     # o servidor escuta aqui (cliente -> servidor)
CLIENT_IP = "127.0.0.1"
CLIENT_PORT = 7676     # o cliente escuta aqui  (servidor -> cliente)
BUFFER_SIZE = 1024 # Limite do tamanho do datagrama

STORAGE_DIR = "./rcvd_files_client/"
PREFIX = "servidor_"
SRC_DIR = "./src/"

def send_file(client_sock, filepath):
    """Lê um arquivo de SRC_DIR e envia ao servidor fragmentado em datagramas."""
    filename = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)

    # Protocolo de envio: b"SAVE" + tamanho (32 bytes big-endian) + nome.extensao codificado
    command = b"SAVE" + file_size.to_bytes(32, "big") + filename.encode("utf-8")
    client_sock.sendto(command, (SERVER_IP, SERVER_PORT))

    n_packets = math.ceil(file_size / BUFFER_SIZE)
    print(f"[ENVIANDO] Iniciando envio de '{filename}' ({file_size} bytes em {n_packets} pacotes)")

    # Envio dos fragmentos
    with open(filepath, "rb") as f:
        for i in range(n_packets):
            chunk = f.read(BUFFER_SIZE)
            client_sock.sendto(chunk, (SERVER_IP, SERVER_PORT))
            print(f"[ENVIANDO] Pacote {i + 1}/{n_packets} enviado para o servidor")

    # Aguarda a string de confirmação ou erro do servidor
    response, _ = client_sock.recvfrom(BUFFER_SIZE)
    resp_text = response.decode("utf-8")
    print(f"[SERVIDOR] {resp_text}")

    return resp_text

def get_file(client_sock, filename):
    """Solicita a devolução do arquivo salvo e o reconstrói no disco."""
    # Protocolo de requisição: b"GET " + nome.extensao codificado
    command = b"GET " + filename.encode("utf-8")
    client_sock.sendto(command, (SERVER_IP, SERVER_PORT))
    print(f"[SOLICITANDO] Requisitando '{filename}' de volta para confirmar recebimento...")

    # A primeira resposta do GET é o tamanho do arquivo em bytes (ou um erro)
    response, _ = client_sock.recvfrom(BUFFER_SIZE)
    resp_text = response.decode("utf-8")

    if resp_text.startswith("ERRO"):
        print(f"[ERRO] {resp_text}")
        return

    file_size = int(resp_text)
    n_packets = math.ceil(file_size / BUFFER_SIZE)
    print(f"[RECEBENDO] Arquivo terá {file_size} bytes. Aguardando {n_packets} pacotes...")

    # O GET pede o nome original. A cópia devolvida é gravada com o PREFIX do cliente
    save_path = os.path.join(STORAGE_DIR, f"{PREFIX}{filename}")

    with open(save_path, "wb") as f:
        for i in range(n_packets):
            chunk, _ = client_sock.recvfrom(BUFFER_SIZE)
            f.write(chunk)
            print(f"[RECEBENDO] Pacote {i + 1}/{n_packets} recebido e gravado")

    print(f"[CONCLUÍDO] Arquivo recuperado com sucesso e salvo como '{save_path}'")

if __name__ == "__main__":
    # O cliente deve aceitar o nome do arquivo como parâmetro de linha de comando
    if len(sys.argv) < 2:
        print(f"Uso correto: python client.py <nome_do_arquivo>   (procurado em {SRC_DIR})")
        sys.exit(1)

    os.makedirs(SRC_DIR, exist_ok=True)
    os.makedirs(STORAGE_DIR, exist_ok=True)

    # O envio deve sair sempre de SRC_DIR
    filename = os.path.basename(sys.argv[1])
    filepath = os.path.join(SRC_DIR, filename)

    if not os.path.isfile(filepath):
        print(f"[ERRO] O arquivo '{filename}' não foi encontrado em '{SRC_DIR}'.")
        sys.exit(1)

    # Criação do socket UDP (fixa CLIENT_PORT)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        client_socket.bind((CLIENT_IP, CLIENT_PORT))
    except OSError as e:
        print(f"[ERRO] Não foi possível escutar em {CLIENT_IP}:{CLIENT_PORT} ({e}).")
        print("       Outro cliente já está em execução?")
        sys.exit(1)

    print(f"[CLIENTE]   Enviando para {SERVER_IP}:{SERVER_PORT} - escutando em {CLIENT_IP}:{CLIENT_PORT}")

    try:
        print("-" * 50)
        # 1. Envia o arquivo (o servidor salva preservando o nome original)
        server_response = send_file(client_socket, filepath)

        # 2. Se o envio foi bem-sucedido, executa a funcionalidade obrigatória de devolução
        if not server_response.startswith("ERRO"):
            print("-" * 50)
            # O servidor preserva o nome original, então o GET pede o mesmo nome
            get_file(client_socket, filename)
            print("-" * 50)

    except Exception as e:
        print(f"[ERRO INESPERADO] {e}")
    finally:
        client_socket.close()
