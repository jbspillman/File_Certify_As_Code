# Security Event Monitoring
from exec_logger import title_large , title_small
from exec_syslog import UDPSyslogServer, TCPSyslogServer, SyslogCapture, setup_syslogger
from exec_secrets import get_creds
from exec_api import API_Connection_Manager
from exec_ssh import SSHClient
from datetime import datetime
import socket
import threading
import json
import time
import os


# Solid lines
lines_80 = '-' * 80   # ---------------
dashd_80 = '─' * 80   # ───────────────
equal_80 = '=' * 80   # ===============
stars_80 = '*' * 80   # ***************
stars_25 = '*' * 25   # ***************
equal_25 = '=' * 25   # ===============

# Dividers
thick  = '▓' * 80     # ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
medium = '▒' * 80     # ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
light  = '░' * 80     # ░░░░░░░░░░░░░░░░░░░░


def start_syslog_server(port=55514, output=None):
    logger = setup_syslogger(verbose=False, silent=True)  # silent=True suppresses console
    capture = SyslogCapture(output or "syslog_capture.log", logger)

    udp = UDPSyslogServer("0.0.0.0", port, capture)
    # tcp = TCPSyslogServer("0.0.0.0", port, capture)

    threading.Thread(target=udp.serve_forever, daemon=True).start()
    # threading.Thread(target=tcp.serve_forever, daemon=True).start()
    return udp, capture

def stop_syslog_server(udp, capture):
    udp.shutdown()
    # tcp.shutdown()
    capture.summary()
    capture.close()

def configure_ontap_event_forwarding(log, port_number=55514):
    
    cluster_details = get_creds(log, "ontap_cluster")
    device = cluster_details["device"]
    usernm = cluster_details["username"]
    passwd = cluster_details["password"]
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))  # doesn't actually send anything
        ip_of_host = s.getsockname()[0]

    
    api_url = f'https://{device}'
    ep = f'/api/security/audit/destinations'
    events_cfg = { 
        "address": f"{ip_of_host}", 
        "facility": "user", 
        "protocol": "udp_unencrypted", 
        "port": port_number, 
        "verify_server": False, 
        "message_format": "rfc_5424", 
        "hostname_format_override": "hostname_only", 
        "timestamp_format_override": "iso_8601_utc"
    }
    log.step('Configuring Event Forwarder to Syslog Server.')
    api = API_Connection_Manager(api_url, username=usernm, password=passwd, verify_ssl=False)
    api.connect()
    code, data = api.post('/api/security/audit/destinations', payload=events_cfg)
    if code == 409 or code == 201:
        skip = True
    else:
        log.info(code)
        log.info(data)

    
    data = api.get('/api/security/audit/destinations')
    if ip_of_host in str(data):
        log.step('Configure Successful.')
    else:
        log.error('Configure Failed.')
    api.disconnect()

def remove_ontap_event_forwarding(log, port_number=55514):
    cluster_details = get_creds(log, "ontap_cluster")
    device = cluster_details["device"]
    usernm = cluster_details["username"]
    passwd = cluster_details["password"]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))  # doesn't actually send anything
        ip_of_host = s.getsockname()[0]

    log.step('Removing Event Forwarder to Syslog Server.')
    api_url = f'https://{device}'
    ep = f'/api/security/audit/destinations'
    api = API_Connection_Manager(api_url, username=usernm, password=passwd, verify_ssl=False)
    api.connect()  
    data = api.delete(f'{ep}/{ip_of_host}/{port_number}')
    if data:
        log.step('Remove Successful.')
    else:
        log.error('Remove Failed.')
    api.disconnect()

def ontap_sem_suite(log, vendor_software):
    log.divider()
    stitle_str = f'{vendor_software} : Security Event Monitoring Tests Starting'
    title_small(log, stitle_str)
    log.divider()
    all_results = []  # This will store results of all tests for all mounts for later reporting and analysis

    ''' add the event forwarding '''
    port_number = 55514
    configure_ontap_event_forwarding(log, port_number)

    ''' start the syslog captures '''
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    log_file = f"{ts}_ontap_sem.log"
    log_file = f'test_syslog.log'
    syslog_server_output = os.path.join('logs', log_file)
    if os.path.exists(syslog_server_output):
        os.remove(syslog_server_output)

    udp, capture = start_syslog_server(port=port_number, output=syslog_server_output)
    log.step(f'Started Syslog Captures on Port: {port_number}')
    log.divider()

    test_ssh_login_capture(log, all_results)


    ''' remove the event forwarding '''
    remove_ontap_event_forwarding(log, port_number)
    time.sleep(5)
    
    ''' stop the syslog captures '''
    log.divider()
    stop_syslog_server(udp, capture)
    log.step('Shutdown Syslog Captures')

    

    phase_1_str = ':admin :: Login Attempt :: Error: Authentication failed.'
    phase_2_str = ':admin :: Login XXXXXXX :: Success?????'
    phase_3_str = ':admin :: Logoff XXXXXXX :: Success?????'
    phase_4_str = ':admin :: Login Attempt :: Error: Authentication failed.'
    phase_5_str = ":admin_wrong :: Login Attempt :: Error: User admin_wrong doesn't exist."
    phase_6_str = ":admin_again :: Login Attempt :: Error: User admin_again doesn't exist."

    p1, p2, p3, p4, p5, p6 = False, False, False, False, False, False
    with open(syslog_server_output, encoding="utf-8") as input:
        lines = input.read()
    for line in lines.split('\n'):
        if ':ssh :: ' in line:
            if phase_1_str in line:
                p1 = True
            if phase_2_str in line:
                p2 = True
            if phase_3_str in line:
                p3 = True
            if phase_4_str in line:
                p4 = True
            if phase_5_str in line:
                p5 = True    
            if phase_6_str in line:
                p6 = True                              
    print(p1, p2, p3, p4, p5, p6)

def log_result(log, test_name: str, test_description: str, passed: bool, message: str = "", all_results = None):
    """Log test result"""

    result = {
        'test': test_name,
        'description': test_description,
        'passed': passed,
        'message': message,
        'timestamp': time.time()
    }
    status = "PASS" if passed else "FAIL"
    log.info(f"[{status}] {test_name}: {message}")

    if all_results is not None:
        all_results.append(result)
    return result


def test_ssh_login_capture(log, all_results):
    """Verify Login Success/Failure are captured for known/unknown accounts."""
    test_name = 'Security Login : Success/Failure'
    test_description = "Confirm that login events for both Success/Failure & Known/Unknown accounts is captured."
  
    log.info(f'{equal_80}')
    log.info(f"TEST: Security Login : Success/Failure")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')

    cluster_details = get_creds(log, "ontap_cluster")
    device = cluster_details["device"]
    usernm = cluster_details["username"]
    passwd = cluster_details["password"]
    
    log.info("Phase 1: SSH Login with valid account, bad password.")
    ssh = SSHClient(
        host     = device,
        username = usernm,
        password = "wrongpassword123"
    )
    result = ssh.connect()
    ssh.disconnect()

    log.info("Phase 2: SSH Login with valid account, good password.")
    ssh = SSHClient(
        host     = device,
        username = usernm,
        password = passwd
    )
    result = ssh.connect()
    ssh.disconnect()
    
    log.info("Phase 3: SSH Logoff system")
    ssh = SSHClient(
        host     = device,
        username = usernm,
        password = passwd
    )
    result = ssh.connect()
    ssh.disconnect()


    log.info("Phase 4: SSH Login Attempt 1 with invalid account, bad password.")
    ssh = SSHClient(
            host     = device,
            username = usernm,
            password = "mottt"
        )
    result = ssh.connect()
    ssh.disconnect()

    log.info("Phase 5: SSH Login Attempt 2 with invalid account [typo], good password.")
    ssh = SSHClient(
        host     = device,
        username = "admin_wrong",
        password = passwd
    )
    result = ssh.connect()
    ssh.disconnect()

    log.info("Phase 6: SSH Login Attempt 3 with invalid account, bad password.")
    ssh = SSHClient(
        host     = device,
        username = "admin_again",
        password = "invalidpswd"
    )
    result = ssh.connect()
    ssh.disconnect()





    # POST /api/support/ems/destinations { "name": "syslogger", "filters": [], "type": "syslog", "destination": "***", "certificate": {}, "syslog": { "port": "55514", "transport": "udp_unencrypted", "format": { "hostname_override": "hostname_only", "message": "rfc_5424", "timestamp_override": "iso_8601_utc" } } }(0.418s)

    # log.info("Phase 7: API Login with valid account, bad password.")
    # log.info("Phase x: API Login with valid account, good password.")
    # log.info("Phase x: API Logoff system")  
    # log.info("Phase x: API Login Attempt 1 with invalid account, bad password.")
    # log.info("Phase x: API Login Attempt 2 with invalid account [typo], good password.")
    # log.info("Phase x: API Login Attempt 3 with invalid account, bad password.")


    # log.info("✓ SEM success")
    # log_result(log, test_name, test_description, True, "SEM success", all_results)

    # log.error("✗ SEM failure")
    # log_result(log, test_name, test_description, False, "SEM failure", all_results)

    # e = "SEM test error 911 911 911"
    # log.error(f"✗ SEM Test failed: {e}")
    # log_result(log, test_name, test_description, False, str(e), all_results)


    # try:

    #     log.info("Phase 1: Reading /proc/mounts")
    #     with open('/proc/mounts', 'r') as f:
    #         mounts = f.read()
    #     log.info(" ✓ Read /proc/mounts successfully")

    #     log.info(f"Phase 2: Searching for mount point: {mount_point}")
    #     mount_line = None
    #     for line in mounts.split('\n'):
    #         if mount_point in line:
    #             mount_line = line
    #             break
        
    #     if not mount_line:
    #         log.error("✗ Mount point not found")
    #         log_result(log, test_name, test_description, False, "Mount not found in /proc/mounts", all_results)
    #         return
    #     log.info(f" ✓ Found the mount point in /proc/mounts")

    #     parts = mount_line.split()
    #     if len(parts) >= 4:
    #         options = parts[3]
    #         log.info(f"Phase 3: Parsing mount options")
            
    #         if 'vers=3' in options or 'nfsvers=3' in options:
    #             log.info(" ✓ NFS Version: 3")
           
    #         if f'proto={mount_options.transport}' in options:
    #             log.info(f" ✓ Transport: {mount_options.transport}")

    #         log.info("✓ Mount options verified")
    #         log_result(log, test_name, test_description, True, "Mount options verified", all_results)
        
    #     else:
    #         log.error("✗ Could not parse mount options")
    #         log_result(log, test_name, test_description, False, "Could not parse mount options", all_results)

    # except Exception as e:
    #     log.error(f"✗ Test failed: {e}")
    #     log_result(log, test_name, test_description, False, str(e), all_results)