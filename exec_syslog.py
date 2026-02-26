"""
Temporary Syslog Capture Server
Listens on UDP/TCP (or both) for syslog messages, logs to console and file.
Stays running until Ctrl+C or end-of-tests signal.
"""

import socket
import socketserver
import threading
import logging
import signal
import sys
import os
import re
import argparse
from datetime import datetime


# ─── Syslog Facility / Severity Decode ───────────────────────────────────────

FACILITIES = {
    0: 'kern', 1: 'user', 2: 'mail', 3: 'daemon', 4: 'auth', 5: 'syslog',
    6: 'lpr', 7: 'news', 8: 'uucp', 9: 'cron', 10: 'authpriv', 11: 'ftp',
    16: 'local0', 17: 'local1', 18: 'local2', 19: 'local3',
    20: 'local4', 21: 'local5', 22: 'local6', 23: 'local7'
}

SEVERITIES = {
    0: 'EMERG', 1: 'ALERT', 2: 'CRIT', 3: 'ERROR',
    4: 'WARN',  5: 'NOTICE', 6: 'INFO', 7: 'DEBUG'
}


def decode_priority(raw: bytes) -> tuple:
    """Parse <PRI> header from syslog message."""
    msg = raw.decode('utf-8', errors='replace').strip()
    match = re.match(r'^<(\d+)>(.*)', msg, re.DOTALL)
    if match:
        pri = int(match.group(1))
        facility = FACILITIES.get(pri >> 3, f'fac{pri >> 3}')
        severity = SEVERITIES.get(pri & 0x7, f'sev{pri & 0x7}')
        body = match.group(2).strip()
        return facility, severity, body
    return 'unknown', 'unknown', msg


# ─── Message Handler ──────────────────────────────────────────────────────────

class SyslogCapture:
    """Central message store and logger."""

    def __init__(self, output_file: str, logger):
        self.logger = logger
        self.output_file = output_file
        self.count = 0
        self.lock = threading.Lock()
        self._file = open(output_file, 'a', buffering=1)  # line-buffered

    def handle(self, data: bytes, client_addr: tuple, proto: str):
        facility, severity, body = decode_priority(data)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        host = client_addr[0]
        port = client_addr[1]

        line = f"[{timestamp}] {proto} {host}:{port} [{facility}.{severity}] {body}"

        with self.lock:
            self.count += 1
            # self.logger.info(line)
            self._file.write(line + '\n')

    def close(self):
        self._file.flush()
        self._file.close()

    def summary(self):
        skip = True
        # self.logger.info(f"Total messages captured: {self.count}")
        # self.logger.info(f"Output written to: {self.output_file}")


# ─── UDP Handler ──────────────────────────────────────────────────────────────

class UDPSyslogHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data, _ = self.request
        self.server.capture.handle(data, self.client_address, 'UDP')


class UDPSyslogServer(socketserver.UDPServer):
    allow_reuse_address = True

    def __init__(self, host, port, capture):
        self.capture = capture
        super().__init__((host, port), UDPSyslogHandler)


# ─── TCP Handler ──────────────────────────────────────────────────────────────

class TCPSyslogHandler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            while True:
                line = self.rfile.readline()
                if not line:
                    break
                self.server.capture.handle(line.strip(), self.client_address, 'TCP')
        except Exception:
            pass


class TCPSyslogServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, host, port, capture):
        self.capture = capture
        super().__init__((host, port), TCPSyslogHandler)


# ─── Main ─────────────────────────────────────────────────────────────────────

def setup_logger(verbose: bool, silent: bool = False) -> logging.Logger:
    logger = logging.getLogger('syslog_capture')
    logger.setLevel(logging.INFO)
    logger.propagate = False  # don't bubble up to root logger

    if verbose: 
    
        fmt = logging.Formatter('%(levelname)s | %(filename)s | %(lineno)d | %(message)s')
    else:
        fmt = logging.Formatter('%(message)s')

    if not silent:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG if verbose else logging.INFO)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger


def main():
    parser = argparse.ArgumentParser(description='Temporary syslog capture server for protocol testing')
    parser.add_argument('--host',    default='0.0.0.0',         help='Bind address (default: 0.0.0.0)')
    parser.add_argument('--port',    type=int, default=55555,      help='Syslog port (default: 55555)')
    parser.add_argument('--proto',   choices=['udp','tcp','both'], default='both', help='Protocol to listen on')
    parser.add_argument('--output',  default=None,               help='Output file (default: syslog_capture_<timestamp>.log)')
    parser.add_argument('--verbose', action='store_true',        help='Verbose output')
    parser.add_argument('--silent',  action='store_true',        help='Suppress console output, log to file only')
    args = parser.parse_args()

    if args.output is None:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output = f'syslog_capture_{ts}.log'

    logger = setup_logger(args.verbose, silent=args.silent)
    capture = SyslogCapture(args.output, logger)

    servers = []
    threads = []

    if args.proto in ('udp', 'both'):
        udp = UDPSyslogServer(args.host, args.port, capture)
        servers.append(udp)
        t = threading.Thread(target=udp.serve_forever, daemon=True)
        t.start()
        threads.append(t)
        logger.info(f"UDP syslog listener started on {args.host}:{args.port}")

    if args.proto in ('tcp', 'both'):
        tcp = TCPSyslogServer(args.host, args.port, capture)
        servers.append(tcp)
        t = threading.Thread(target=tcp.serve_forever, daemon=True)
        t.start()
        threads.append(t)
        logger.info(f"TCP syslog listener started on {args.host}:{args.port}")

    logger.info(f"Capturing to: {args.output}")
    logger.info("Press Ctrl+C to stop and finalize capture\n")

    def shutdown(sig, frame):
        logger.info("\n─── Shutting down ───")
        for s in servers:
            s.shutdown()
        capture.summary()
        capture.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Keep main thread alive
    signal.pause()


if __name__ == '__main__':
    main()
