"""
Canal de transferência confiável RDT 3.0 (Stop-and-Wait) sobre sockets UDP.

Cabeçalho de 4 bytes: [TIPO (1)] [SEQ (1)] [CHECKSUM (2)] [DADOS]

O transmissor e o receptor são máquinas separadas, como no modelo, e cada uma
só enxerga os pacotes que lhe dizem respeito. Quem faz essa divisão é o
despacho em _despacha, o único ponto do código que lê o socket. Ele olha o
tipo do pacote e manda o ACK para o transmissor e os dados para o receptor.

A simulação do canal está em send_pkt, um pacote entregue a ela pode sumir ou
ter um bit trocado antes de chegar ao destino.
"""

import random
import socket
import time

# Configurações do RDT
LOSS_PROB = 0.15      # 15% de probabilidade de perda no canal
CORRUPT_PROB = 0.15   # 15% de probabilidade de corrupção no canal
TIMEOUT = 0.05        # 50ms de timeout
VERBOSE = True        # Registra cada evento do protocolo em tempo de execução

HEADER_SIZE = 4
ESPERA_CURTA = 0.05   # quanto o despacho fica parado quando não há prazo correndo


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
    def __init__(self, sock):
        self.sock = sock
        self.seq_send = 0   # estado da máquina do transmissor
        self.seq_recv = 0   # estado da máquina do receptor
        self.entregues = []
        self.stats = self._zerar_stats()

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

    def log(self, mensagem):
        if VERBOSE:
            print(f"[RDT]       {mensagem}", flush=True)

    def log_canal(self, mensagem):
        if VERBOSE:
            print(f"[RDT]       (canal) {mensagem}", flush=True)

    def print_and_clear_stats(self):
        s = self.stats
        print(
            f"[RDT 3.0] Resumo -> Timeouts: {s['timeouts']} | "
            f"Retransmissões: {s['retransmissoes']} | "
            f"ACKs reenviados: {s['acks_repetidos']} | "
            f"ACKs ignorados: {s['acks_ignorados']} | "
            f"Perdas no canal: {s['perdas']} | Corrupções no canal: {s['corrupcoes']} | "
            f"Corrompidos detectados: {s['corrompidos_detectados']}"
        )
        self.stats = self._zerar_stats()

    def send_pkt(self, pkt, addr):
        """Entrega o pacote ao canal, que pode perdê-lo ou corrompê-lo.

        O mesmo sorteio decide as duas coisas, então elas são exclusivas e cada
        uma mantém a probabilidade configurada.
        """
        sorteio = random.random()

        if sorteio < LOSS_PROB:
            self.stats["perdas"] += 1
            self.log_canal(f"PERDEU {self._descrever(pkt)}")
            return

        if sorteio < LOSS_PROB + CORRUPT_PROB:
            self.stats["corrupcoes"] += 1
            self.log_canal(f"CORROMPEU {self._descrever(pkt)}")
            pkt = self._corromper(pkt)

        self.sock.sendto(pkt, addr)

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

    def _despacha(self, prazo):
        """Lê um pacote do socket e o encaminha à máquina a que ele pertence.

        Devolve (seq, integro) quando o pacote é um ACK, que só interessa ao
        transmissor, e None quando é dado ou quando a espera acabou sem nada.
        O prazo é o instante em que o temporizador do transmissor dispara, ou
        None quando nenhum envio está em curso.
        """
        restante = (prazo - time.monotonic()) if prazo else ESPERA_CURTA
        if restante <= 0:
            return None

        try:
            self.sock.settimeout(restante)
            rcvpkt, addr = self.sock.recvfrom(65535)
        except socket.timeout:
            return None

        pkt_type, seq, data, integro = parse_pkt(rcvpkt)

        if pkt_type == b"A":
            return seq, integro

        self._receptor(pkt_type, seq, data, integro, addr)
        return None

    def _receptor(self, pkt_type, seq, data, integro, addr):
        """Máquina do receptor, que o despacho aciona a cada dado que chega."""
        esperado = self.seq_recv

        # ------------------------------------------------------------------
        # Estado "Esperar N de baixo"
        #
        #   rdt_rcv(rcvpkt) && (corrupt(rcvpkt) || has_seq(1-N)(rcvpkt))
        #     sndpkt = make_pkt(ACK, 1-N, checksum)
        #     udt_send(sndpkt)
        # ------------------------------------------------------------------
        if not integro or pkt_type != b"D" or seq != esperado:
            anterior = 1 - esperado
            if not integro:
                self.stats["corrompidos_detectados"] += 1
                self.log(f"checksum inválido, reenvia ACK {anterior}")
            else:
                self.log(f"pacote {seq} duplicado, reenvia ACK {anterior}")
            self.stats["acks_repetidos"] += 1
            self.send_pkt(make_pkt(b"A", anterior), addr)
            return

        # ------------------------------------------------------------------
        #   rdt_rcv(rcvpkt) && notcorrupt(rcvpkt) && has_seqN(rcvpkt)
        #     extract(rcvpkt, data)
        #     deliver_data(data)
        #     sndpkt = make_pkt(ACK, N, checksum)
        #     udt_send(sndpkt)
        #
        # Vai para "Esperar 1-N de baixo".
        # ------------------------------------------------------------------
        self.seq_recv = 1 - esperado
        self.log(f"pacote {esperado} íntegro, envia ACK {esperado}")
        self.entregues.append((data, addr))
        self.send_pkt(make_pkt(b"A", esperado), addr)

    def rdt_send(self, data, addr):
        """Máquina do transmissor."""
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
        self.send_pkt(sndpkt, addr)
        prazo = time.monotonic() + TIMEOUT

        # ------------------------------------------------------------------
        # Estado "Esperar ACK N"
        #
        # O temporizador é armado no envio e rearmado só quando dispara, então
        # nada que chegue no meio do caminho estica o prazo.
        # ------------------------------------------------------------------
        while True:
            ack = self._despacha(prazo)

            if ack is None:
                if time.monotonic() >= prazo:
                    # ----------------------------------------------------
                    #   timeout
                    #     udt_send(sndpkt)
                    #     start_timer
                    # ----------------------------------------------------
                    self.stats["timeouts"] += 1
                    self.stats["retransmissoes"] += 1
                    self.log("timeout, retransmite")
                    self.send_pkt(sndpkt, addr)
                    prazo = time.monotonic() + TIMEOUT
                continue

            ack_seq, integro = ack

            if not integro or ack_seq != seq:
                # --------------------------------------------------------
                #   rdt_rcv(rcvpkt) && (corrupt(rcvpkt) || isACK(rcvpkt, 1-N))
                #     Λ
                # --------------------------------------------------------
                if not integro:
                    self.stats["corrompidos_detectados"] += 1
                    self.log("ACK corrompido, ignora")
                else:
                    self.stats["acks_ignorados"] += 1
                    self.log(f"ACK {ack_seq} fora de ordem, ignora")
                continue

            # ------------------------------------------------------------
            #   rdt_rcv(rcvpkt) && notcorrupt(rcvpkt) && isACK(rcvpkt, N)
            #     stop_timer
            #
            # Vai para "Esperar chamada 1-N de cima".
            # ------------------------------------------------------------
            self.seq_send = 1 - seq
            self.log(f"ACK {seq} recebido")
            return

    def rdt_recv(self):
        """Entrega à camada de cima o próximo dado que o receptor aceitou."""
        while not self.entregues:
            # ------------------------------------------------------------
            # Estado "Esperar chamada N de cima", do lado do transmissor
            #
            #   rdt_rcv(rcvpkt)
            #     Λ
            #
            # Nenhum envio está em curso, então o ACK atrasado que o despacho
            # devolver aqui é descartado.
            # ------------------------------------------------------------
            if self._despacha(None) is not None:
                self.stats["acks_ignorados"] += 1
                self.log("ACK atrasado fora de envio, ignora")

        return self.entregues.pop(0)
