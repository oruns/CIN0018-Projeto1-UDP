"""
Canal de transferência confiável RDT 3.0 (Stop-and-Wait) sobre sockets UDP.

Cabeçalho de 4 bytes: [TIPO (1)] [SEQ (1)] [CHECKSUM (2)] [DADOS]

As duas máquinas de estado do protocolo são independentes e ficam ativas ao
mesmo tempo, como no modelo. Cada lado abre um socket para o papel de
transmissor e outro para o de receptor, então o socket que espera ACK nunca
recebe dados e o que espera dados nunca recebe ACK.

A máquina do receptor roda na própria thread, sempre escutando, e põe numa
fila o que entrega para a camada de cima. É o que permite ao lado que está
transmitindo continuar atendendo as retransmissões do parceiro.

A simulação do canal está em send_pkt, um pacote entregue a ela pode sumir ou
ter um bit trocado antes de chegar ao destino.
"""

import queue
import random
import socket
import threading
import time

# Configurações do RDT
LOSS_PROB = 0.15      # 15% de probabilidade de perda no canal
CORRUPT_PROB = 0.15   # 15% de probabilidade de corrupção no canal
TIMEOUT = 0.05        # 50ms de timeout
VERBOSE = True        # Registra cada evento do protocolo em tempo de execução

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
    """Monta o pacote com o checksum de tipo, seq e dados."""
    protegido = pkt_type + bytes([seq]) + data
    return pkt_type + bytes([seq]) + checksum(protegido).to_bytes(2, "big") + data


def parse_pkt(pkt):
    """Desmonta o pacote e confere o checksum.

    Devolve (tipo, seq, dados, integro); tipo é None se vier menor que o cabeçalho.
    """
    if len(pkt) < HEADER_SIZE:
        return None, None, b"", False

    pkt_type = pkt[0:1]
    seq = pkt[1]
    recebido = int.from_bytes(pkt[2:4], "big")
    data = pkt[HEADER_SIZE:]

    return pkt_type, seq, data, checksum(pkt_type + bytes([seq]) + data) == recebido


class RDTSocket:
    def __init__(self, sock_envio, sock_recepcao):
        self.sock_envio = sock_envio
        self.sock_recepcao = sock_recepcao
        self.sock_recepcao.settimeout(TIMEOUT)

        self.seq_send = 0   # estado da máquina do transmissor
        self.seq_recv = 0   # estado da máquina do receptor

        self.trava = threading.Lock()
        self.stats = self._zerar_stats()
        self.entregues = queue.Queue()

        threading.Thread(target=self._maquina_receptor, daemon=True).start()

    @staticmethod
    def _zerar_stats():
        return {
            "timeouts": 0,
            "acks_repetidos": 0,
            "acks_ignorados": 0,
            "retransmissoes": 0,
            "perdas": 0,
            "corrupcoes": 0,
            "corrompidos_detectados": 0,
        }

    def conta(self, chave):
        """Incrementa um contador, que as duas máquinas compartilham."""
        with self.trava:
            self.stats[chave] += 1

    def log(self, mensagem):
        if VERBOSE:
            print(f"[RDT]       {mensagem}", flush=True)

    def log_canal(self, mensagem):
        if VERBOSE:
            print(f"[RDT]       (canal) {mensagem}", flush=True)

    def print_and_clear_stats(self):
        with self.trava:
            s, self.stats = self.stats, self._zerar_stats()
        print(
            f"[RDT 3.0] Resumo -> Timeouts: {s['timeouts']} | "
            f"Retransmissões: {s['retransmissoes']} | "
            f"ACKs reenviados: {s['acks_repetidos']} | "
            f"ACKs ignorados: {s['acks_ignorados']} | "
            f"Perdas no canal: {s['perdas']} | Corrupções no canal: {s['corrupcoes']} | "
            f"Corrompidos detectados: {s['corrompidos_detectados']}"
        )

    def send_pkt(self, pkt, sock, addr):
        """Entrega o pacote ao canal, que pode perdê-lo ou corrompê-lo.

        O mesmo sorteio decide as duas coisas, então elas são exclusivas e cada
        uma mantém a probabilidade configurada.
        """
        sorteio = random.random()

        if sorteio < LOSS_PROB:
            self.conta("perdas")
            self.log_canal(f"PERDEU {self._descrever(pkt)}")
            return

        if sorteio < LOSS_PROB + CORRUPT_PROB:
            self.conta("corrupcoes")
            self.log_canal(f"CORROMPEU {self._descrever(pkt)}")
            pkt = self._corromper(pkt)

        sock.sendto(pkt, addr)

    @staticmethod
    def _descrever(pkt):
        rotulo = {b"D": "dados", b"A": "ACK"}.get(pkt[0:1], "pacote")
        return f"{rotulo} seq={pkt[1]}"

    @staticmethod
    def _corromper(pkt):
        """Inverte um bit qualquer do pacote, cabeçalho incluído."""
        corrompido = bytearray(pkt)
        posicao = random.randrange(len(corrompido))
        corrompido[posicao] ^= 1 << random.randrange(8)
        return bytes(corrompido)

    def rdt_send(self, data, addr):
        """Máquina do transmissor, que vai do envio até a confirmação."""
        seq = self.seq_send
        sndpkt = make_pkt(b"D", seq, data)

        # ------------------------------------------------------------------
        # Estado "Esperar chamada N de cima"
        #
        #   rdt_send(data)
        #     sndpkt = make_pkt(N, data, checksum)
        #     udt_send(sndpkt)
        #     start_timer
        # ------------------------------------------------------------------
        self.log(f"envia pacote {seq}")
        self.send_pkt(sndpkt, self.sock_envio, addr)
        prazo = time.monotonic() + TIMEOUT

        # ------------------------------------------------------------------
        # Estado "Esperar ACK N"
        #
        #   rdt_rcv(rcvpkt) && (corrupt(rcvpkt) || isACK(rcvpkt, 1-N))
        #     Λ
        #
        #   timeout
        #     udt_send(sndpkt)
        #     start_timer
        #
        #   rdt_rcv(rcvpkt) && notcorrupt(rcvpkt) && isACK(rcvpkt, N)
        #     stop_timer
        #
        # O temporizador é armado no envio e só é rearmado quando dispara, de
        # modo que um pacote qualquer que chegue no meio não estica o prazo.
        # ------------------------------------------------------------------
        while True:
            restante = prazo - time.monotonic()
            try:
                if restante <= 0:
                    raise socket.timeout
                self.sock_envio.settimeout(restante)
                rcvpkt, _ = self.sock_envio.recvfrom(65535)
            except socket.timeout:
                self.conta("timeouts")
                self.conta("retransmissoes")
                self.log("timeout, retransmite")
                self.send_pkt(sndpkt, self.sock_envio, addr)
                prazo = time.monotonic() + TIMEOUT
                continue

            pkt_type, rcv_seq, _, integro = parse_pkt(rcvpkt)

            if not integro:
                self.conta("corrompidos_detectados")
                self.log("ACK corrompido, ignora")
                continue

            if pkt_type != b"A" or rcv_seq != seq:
                self.conta("acks_ignorados")
                self.log(f"ACK {rcv_seq} fora de ordem, ignora")
                continue

            self.seq_send = 1 - seq
            self.log(f"ACK {seq} recebido")
            return

    def rdt_recv(self):
        """Entrega à camada de cima o próximo dado que a máquina do receptor aceitou."""
        return self.entregues.get()

    def _maquina_receptor(self):
        """Máquina do receptor, que fica ativa enquanto o programa roda."""
        while True:
            esperado = self.seq_recv

            try:
                rcvpkt, addr = self.sock_recepcao.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return

            pkt_type, seq, data, integro = parse_pkt(rcvpkt)

            # --------------------------------------------------------------
            # Estado "Esperar N de baixo"
            #
            #   rdt_rcv(rcvpkt) && (corrupt(rcvpkt) || has_seq(1-N)(rcvpkt))
            #     sndpkt = make_pkt(ACK, 1-N, checksum)
            #     udt_send(sndpkt)
            # --------------------------------------------------------------
            if not integro or pkt_type != b"D" or seq != esperado:
                anterior = 1 - esperado
                if not integro:
                    self.conta("corrompidos_detectados")
                    self.log(f"checksum inválido, reenvia ACK {anterior}")
                else:
                    self.log(f"pacote {seq} duplicado, reenvia ACK {anterior}")
                self.conta("acks_repetidos")
                self.send_pkt(make_pkt(b"A", anterior), self.sock_recepcao, addr)
                continue

            # --------------------------------------------------------------
            #   rdt_rcv(rcvpkt) && notcorrupt(rcvpkt) && has_seqN(rcvpkt)
            #     extract(rcvpkt, data)
            #     deliver_data(data)
            #     sndpkt = make_pkt(ACK, N, checksum)
            #     udt_send(sndpkt)
            # --------------------------------------------------------------
            self.seq_recv = 1 - esperado
            self.log(f"pacote {esperado} íntegro, envia ACK {esperado}")
            self.entregues.put((data, addr))
            self.send_pkt(make_pkt(b"A", esperado), self.sock_recepcao, addr)
