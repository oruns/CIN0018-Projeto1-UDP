"""
Implementação do canal de transferência confiável RDT 3.0 (Stop-and-Wait)
sobre sockets UDP, conforme o modelo do Kurose.

Cada pacote carrega um cabeçalho de 4 bytes:

    [TIPO (1 byte)] [SEQ (1 byte)] [CHECKSUM (2 bytes)] [DADOS]

O protocolo não usa NAK. Quando um pacote chega corrompido ou fora de ordem,
o receptor responde reenviando o ACK do último pacote que recebeu corretamente;
o remetente lê esse ACK duplicado como pedido de retransmissão. É assim que o
RDT 2.2 dispensa o NAK, e o RDT 3.0 herda esse desenho acrescentando o
temporizador que cobre a perda total do pacote.

O canal (send_pkt) é quem simula o ambiente hostil: um pacote entregue a ele
pode sumir ou ter bits trocados antes de chegar ao destino.
"""

import socket
import random

# Configurações do RDT
LOSS_PROB = 0.15      # 15% de probabilidade de o canal perder o pacote
CORRUPT_PROB = 0.15   # 15% de probabilidade de o canal corromper o pacote
TIMEOUT = 0.05        # 50ms de timeout

# Imprime cada evento do protocolo em tempo de execução. Pode ser desligado
# quando o volume de linhas atrapalhar a leitura de transferências grandes.
VERBOSE = True

HEADER_SIZE = 4


def checksum(data):
    """Soma de verificação de 16 bits em complemento de um."""
    total = 0
    for i in range(0, len(data) - 1, 2):
        total += (data[i] << 8) + data[i + 1]
    if len(data) % 2:
        total += data[-1] << 8
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return ~total & 0xFFFF


def make_pkt(pkt_type, seq, data=b""):
    """Monta um pacote já com o checksum calculado sobre tipo, seq e dados."""
    protegido = pkt_type + bytes([seq]) + data
    return pkt_type + bytes([seq]) + checksum(protegido).to_bytes(2, "big") + data


def parse_pkt(pkt):
    """Desmonta um pacote e confere o checksum.

    Devolve (tipo, seq, dados, integro). Um pacote menor que o cabeçalho é
    tratado como lixo e devolve tipo None.
    """
    if len(pkt) < HEADER_SIZE:
        return None, None, b"", False

    pkt_type = pkt[0:1]
    seq = pkt[1]
    recebido = int.from_bytes(pkt[2:4], "big")
    data = pkt[HEADER_SIZE:]

    integro = checksum(pkt_type + bytes([seq]) + data) == recebido
    return pkt_type, seq, data, integro


class RDTSocket:
    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(TIMEOUT)
        self.seq_send = 0
        self.seq_recv = 0
        self.recv_buffer = []
        self.stats = self._zerar_stats()

    @staticmethod
    def _zerar_stats():
        return {
            "timeouts": 0,
            "acks_repetidos": 0,    # ACKs que o receptor reenviou
            "acks_duplicados": 0,   # ACKs duplicados que o remetente leu
            "retransmissoes": 0,
            "perdas": 0,
            "corrupcoes": 0,
            "corrompidos_detectados": 0,
        }

    def log(self, mensagem):
        """Registra um evento do protocolo no momento em que ele acontece."""
        if VERBOSE:
            print(f"[RDT]       {mensagem}", flush=True)

    def print_and_clear_stats(self):
        s = self.stats
        print(
            f"[RDT 3.0] Resumo -> Timeouts: {s['timeouts']} | "
            f"Retransmissões: {s['retransmissoes']} | "
            f"ACKs reenviados: {s['acks_repetidos']} | "
            f"ACKs duplicados recebidos: {s['acks_duplicados']} | "
            f"Perdas no canal: {s['perdas']} | Corrupções no canal: {s['corrupcoes']} | "
            f"Corrompidos detectados: {s['corrompidos_detectados']}"
        )
        self.stats = self._zerar_stats()

    def send_pkt(self, pkt, addr):
        """Entrega o pacote ao canal, que pode perdê-lo ou corrompê-lo.

        Perda e corrupção são sorteadas de um mesmo valor, portanto são
        mutuamente exclusivas e cada uma mantém a probabilidade configurada.
        """
        sorteio = random.random()

        if sorteio < LOSS_PROB:
            self.stats["perdas"] += 1
            self.log(f"canal PERDEU {self._descrever(pkt)}")
            return

        if sorteio < LOSS_PROB + CORRUPT_PROB:
            self.stats["corrupcoes"] += 1
            self.log(f"canal CORROMPEU {self._descrever(pkt)}")
            pkt = self._corromper(pkt)

        self.sock.sendto(pkt, addr)

    @staticmethod
    def _descrever(pkt):
        rotulo = {b"D": "dados", b"A": "ACK"}.get(pkt[0:1], "pacote")
        return f"{rotulo} seq={pkt[1]}"

    @staticmethod
    def _corromper(pkt):
        """Inverte um bit do pacote, preservando apenas o byte de tipo.

        O número de sequência entra na área corrompível porque o receptor não
        confia nele quando o checksum falha: a resposta é sempre o ACK do
        último pacote aceito, que ele conhece pelo próprio estado.
        """
        corrompido = bytearray(pkt)
        posicao = random.randrange(1, len(corrompido))
        corrompido[posicao] ^= 1 << random.randrange(8)
        return bytes(corrompido)

    def rdt_send(self, data, addr):
        """Envia dados de forma confiável usando RDT 3.0 (Stop-and-Wait).

        Retransmite enquanto não receber o ACK correspondente: o protocolo não
        descarta dados, de modo que a entrega independe da taxa de perda.
        """
        pkt = make_pkt(b"D", self.seq_send, data)
        primeira_tentativa = True

        while True:
            if not primeira_tentativa:
                self.stats["retransmissoes"] += 1
            self.send_pkt(pkt, addr)
            primeira_tentativa = False

            # Aguarda a confirmação dentro da janela do temporizador.
            aguardando = True
            while aguardando:
                try:
                    recv_pkt, recv_addr = self.sock.recvfrom(65535)
                except socket.timeout:
                    self.stats["timeouts"] += 1
                    self.log(f"TIMEOUT esperando ACK {self.seq_send} - retransmitindo")
                    aguardando = False
                    continue

                pkt_type, seq, _, integro = parse_pkt(recv_pkt)

                if not integro:
                    # ACK ilegível: aguarda o temporizador, como no modelo.
                    self.stats["corrompidos_detectados"] += 1
                    self.log("ACK chegou corrompido - aguardando timeout")
                    continue

                if pkt_type == b"A":
                    if seq == self.seq_send:
                        self.log(f"ACK {seq} recebido")
                        self.seq_send = 1 - self.seq_send
                        return
                    # ACK do pacote anterior: o receptor está pedindo de novo.
                    self.stats["acks_duplicados"] += 1
                    self.log(f"ACK {seq} duplicado - retransmitindo")
                    aguardando = False

                elif pkt_type == b"D":
                    # Chegou um pacote de dados enquanto esperávamos a
                    # confirmação, porque os dois sentidos dividem o socket.
                    if seq == self.seq_recv:
                        # Pacote novo: guarda para o rdt_recv processar.
                        self.recv_buffer.append((recv_pkt, recv_addr))
                    else:
                        # Pacote antigo: confirma de novo para destravar o par.
                        self.send_pkt(make_pkt(b"A", seq), recv_addr)

    def rdt_recv(self):
        """Recebe dados de forma confiável usando RDT 3.0.

        Todo pacote que não seja o esperado e íntegro provoca o reenvio do ACK
        do último pacote aceito, que é o que substitui o NAK.
        """
        while True:
            if self.recv_buffer:
                pkt, addr = self.recv_buffer.pop(0)
            else:
                try:
                    pkt, addr = self.sock.recvfrom(65535)
                except socket.timeout:
                    continue

            pkt_type, seq, data, integro = parse_pkt(pkt)

            if pkt_type != b"D":
                # ACKs atrasados não interessam a quem espera dados.
                continue

            if not integro:
                # O seq do pacote corrompido não é confiável, então a resposta
                # vem do estado local: o ACK do último pacote aceito.
                ultimo_aceito = 1 - self.seq_recv
                self.stats["corrompidos_detectados"] += 1
                self.stats["acks_repetidos"] += 1
                self.log(
                    f"pacote com checksum inválido - reenviando ACK {ultimo_aceito}"
                )
                self.send_pkt(make_pkt(b"A", ultimo_aceito), addr)
                continue

            if seq == self.seq_recv:
                self.log(f"pacote {seq} íntegro - enviando ACK {seq}")
                self.send_pkt(make_pkt(b"A", seq), addr)
                self.seq_recv = 1 - self.seq_recv
                return data, addr

            # Duplicado: o ACK anterior se perdeu, então confirma de novo.
            self.stats["acks_repetidos"] += 1
            self.log(f"pacote {seq} duplicado - reenviando ACK {seq}")
            self.send_pkt(make_pkt(b"A", seq), addr)
