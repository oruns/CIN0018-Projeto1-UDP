import socket
import os
import sys
import math

# Configurações de rede compatíveis com o servidor
SERVER_IP = "127.0.0.1"
SERVER_PORT = 6767
BUFFER_SIZE = 1024 # Limite do tamanho do datagrama

def send_file(client_sock, filepath):
    """Lê um arquivo local e envia ao servidor fragmentado em datagramas."""
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

    # Salva com o prefixo 'recebido_' para não sobrescrever o arquivo original do cliente
    save_path = f"recebido_{filename}"
    
    with open(save_path, "wb") as f:
        for i in range(n_packets):
            chunk, _ = client_sock.recvfrom(BUFFER_SIZE)
            f.write(chunk)
            print(f"[RECEBENDO] Pacote {i + 1}/{n_packets} recebido e gravado")

    print(f"[CONCLUÍDO] Arquivo recuperado com sucesso e salvo como '{save_path}'")

if __name__ == "__main__":
    # O cliente deve aceitar o nome do arquivo como parâmetro de linha de comando
    if len(sys.argv) < 2:
        print("Uso correto: python client.py <caminho_do_arquivo>")
        sys.exit(1)

    filepath = sys.argv[1]
    
    if not os.path.isfile(filepath):
        print(f"[ERRO] O arquivo '{filepath}' não foi encontrado localmente.")
        sys.exit(1)

    # Criação do socket UDP
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        print("-" * 50)
        # 1. Envia o arquivo (o servidor salvará com o prefixo "cliente_")
        server_response = send_file(client_socket, filepath)

        # 2. Se o envio foi bem-sucedido, executa a funcionalidade obrigatória de devolução
        if not server_response.startswith("ERRO"):
            print("-" * 50)
            original_filename = os.path.basename(filepath)
            # O servidor sempre salva usando o prefixo "cliente_"
            stored_filename = f"cliente_{original_filename}"
            get_file(client_socket, stored_filename)
            print("-" * 50)
            
    except Exception as e:
        print(f"[ERRO INESPERADO] {e}")
    finally:
        client_socket.close()