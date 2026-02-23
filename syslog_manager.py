from a_syslog_server import setup_logger, UDPSyslogServer, TCPSyslogServer, SyslogCapture
import threading
import time
import os



def start_syslog_server(syslog_folder, port=55555, verbose=False, silent=False):
    logger = setup_logger(verbose=verbose)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    # syslog_file_path = os.path.join(syslog_folder, f'{timestamp}_syslogger_{port}.log')
    
    syslog_file_path = os.path.join(syslog_folder, f'TEMPFILE_syslogger_{port}.log')
    if os.path.exists(syslog_file_path): # Create the file if it doesn't exist
        os.remove(syslog_file_path)  # Remove existing file to start fresh

    if not os.path.exists(syslog_file_path): # Create the file if it doesn't exist
        open(syslog_file_path, 'a').close()  
    os.chmod(syslog_file_path, 0o777)        # Ensure the file is writable by all users

    capture = SyslogCapture(syslog_file_path, logger)
    
    udp = UDPSyslogServer('0.0.0.0', port, capture)
    tcp = TCPSyslogServer('0.0.0.0', port, capture)
    
    threading.Thread(target=udp.serve_forever, daemon=True).start()
    threading.Thread(target=tcp.serve_forever, daemon=True).start()
    
    return udp, tcp, capture, syslog_file_path  # return handles for shutdown


def stop_syslog_server(udp, tcp, capture, syslog_file_path=None):
    udp.shutdown()
    tcp.shutdown()
    capture.summary()
    capture.close()
    if syslog_file_path and os.path.exists(syslog_file_path):
        os.chmod(syslog_file_path, 0o777)  # Ensure the file is writable by all users before deletion
    time.sleep(6)       

