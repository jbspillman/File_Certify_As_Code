from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import multiprocessing
import subprocess
import tempfile
import logging
import fcntl
import json
import time
import sys
import os

from a_api_manager import API_Connection_Manager
from a_ssh_manager import SSHClient

logger = logging.getLogger(__name__)


''' some general constants '''
stars_25 = '*' * 25
equal_25 = '=' * 25
equal_80 = '=' * 80
''' end of general constants '''


"""
This will configure the Storage Appliances to send syslog messages to this application, 
which will capture them and log them to a file for later analysis and correlation with the test results.
"""



def audit_configure(logger, storage_appliance, audit_action):
    logger.info("")
    logger.info(f'{equal_80}')
    logger.info("AUDIT LOG GENERATION TEST SUITE")
    logger.info(f'{equal_80}')
    logger.info("")

    vendor = storage_appliance['vendor']
    software = storage_appliance['software']
    management_device = storage_appliance['management_device']
    management_username = storage_appliance['management_username']
    management_password = storage_appliance['management_password']
    syslog_server = storage_appliance['syslog_server']
    syslog_port = storage_appliance['syslog_port']
    syslog_protocol = storage_appliance['syslog_protocol']

    if audit_action == None or len(audit_action) == 0:
        logger.error(f"No audit actions specified for {vendor} {software} @ ({management_device}), skipping audit actions")
        return False

    start_time = time.time()
    if 'DELL' in vendor.upper():
        audit_url = f"https://{management_device}:8080/platform"
        audit_endpoint = f"/audit/settings/global"
        audit_hostname = management_device.split('.')[0].split('-')[0]
        
        logger.info("")
        logger.info(f"Starting AUDIT Actions for {vendor} {software} : Cluster: {audit_hostname} @ ({management_device})")
        logger.info("")

        clear_audit_payload = {'audited_zones': [], 'auto_purging_enabled': False, 'cee_server_uris': [], 'config_auditing_enabled': False, 'config_syslog_enabled': False, 'config_syslog_servers': [], 'hostname': '', 'protocol_auditing_enabled': False, 'protocol_syslog_servers': [], 'retention_period': 180, 'system_syslog_enabled': False, 'system_syslog_servers': []}
        configure_audit_payload = configure_audit_payload={f"hostname": f"{audit_hostname}","audited_zones": ["System"],"auto_purging_enabled": True,"retention_period": 7,"config_auditing_enabled": True,"config_syslog_enabled": True,"config_syslog_servers": [f"{syslog_server}:{syslog_port}"],"protocol_auditing_enabled": True,"protocol_syslog_servers": [f"{syslog_server}:{syslog_port}"],"protocol_syslog_tls_enabled": False,"system_auditing_enabled": True,"system_syslog_enabled": True,"system_syslog_servers": [f"{syslog_server}:{syslog_port}"]}
        api_audit_commands = [
            ('Get Audit Settings', 'GET', audit_endpoint, None),
            ('Delete Audit Settings', 'PUT', audit_endpoint, clear_audit_payload),
            ('Set Audit Settings', 'PUT', audit_endpoint, configure_audit_payload),
            ('Verify Audit Settings', 'GET', audit_endpoint, None),
            ('Delete Audit Settings', 'PUT', audit_endpoint, clear_audit_payload),
        ]
        return True

    
    if 'NETAPP' in vendor.upper():
        audit_url = f"https://{management_device}/api"
        audit_endpoint = f"/security/audit/destinations"
        audit_extra_data = f'fields=**&return_records=true&return_timeout=120'
        audit_hostname = management_device.split('-')[0]
        if 'udp' in syslog_protocol.lower():  # <udp-unencrypted|tcp-unencrypted|tcp-encrypted>
            syslog_protocol = 'udp_unencrypted'
        else:
            syslog_protocol = 'tcp_unencrypted'

        configure_audit_payload = {f"address": f"{syslog_server}", "port": f"{syslog_port}", "protocol": f"{syslog_protocol}", "facility": "user", "verify_server": False, "message_format": "rfc_5424", "hostname_format_override": "hostname_only", "timestamp_format_override": "iso_8601_utc"}
        api_audit_commands = [
            ('Get Audit Settings', 'GET', f"{audit_endpoint}?{audit_extra_data}", None),
            ('Delete Audit Settings', 'DELETE', f"{audit_endpoint}/{syslog_server}/{syslog_port}", None),
            ('Set Audit Settings', 'POST', audit_endpoint, configure_audit_payload),
            ('Verify Audit Settings', 'GET', f"{audit_endpoint}?{audit_extra_data}", None),
            ('Get Audit Settings', 'GET', f"{audit_endpoint}?{audit_extra_data}", None),
            ('Delete Audit Settings', 'DELETE', f"{audit_endpoint}/{syslog_server}/{syslog_port}", None)
        ]

        temp_commands = []
        for command_name, method, endpoint, payload in api_audit_commands:
            if command_name in audit_action:
                if (command_name, method, endpoint, payload) not in temp_commands:
                    temp_commands.append((command_name, method, endpoint, payload))
        api_audit_commands = temp_commands  # filter to only the specified actions, e.g. ['Set Audit Settings', 'Verify Audit Settings'] if you only want to set and verify the settings without the get and delete steps.

        """ OPEN THE API CONNECTION """
        with API_Connection_Manager(audit_url, username=management_username, password=management_password, verify_ssl=False) as api:
            for command_name, method, endpoint, payload in api_audit_commands:
                if method == 'GET':
                    data = api.get(endpoint)
                    if data:
                        if command_name != 'Verify Audit Settings':
                            logger.info(f"Successfully executed '{command_name}' on {vendor} {software} @ ({management_device}) | ")
                            if data["num_records"] == 0:
                                logger.info(f"No audit syslog destinations currently configured on {vendor} {software} @ ({management_device}) | ")
                            else:                               
                                logger.info(f"Current audit syslog destinations on {vendor} {software} @ ({management_device}) | ")
                                for record in data["records"]:
                                    logger.info(f" - {record['address']}:{record['port']} ({record['protocol']})")
                        
                        elif command_name == 'Verify Audit Settings':
                            data_as_string = str(data).lower()
                            found_all = 0
                            for word in data_as_string.split():
                                if syslog_server.lower() in word or str(syslog_port) in word or syslog_protocol.lower() in word:
                                    found_all += 1                       
                            if found_all >= 3:
                                logger.info(f"Successfully executed '{command_name}' on {vendor} {software} @ ({management_device}) | ")
                            else:
                                logger.error(f"Failed to execute '{command_name}' settings NOT found in audit settings from {vendor} {software} @ ({management_device}) | ")
                                logger.error(f"Expected to find: {syslog_server} AND {syslog_port} AND ({syslog_protocol})")
                                logger.error(f"Actual settings: {json.dumps(data, indent=4)}")  
                        else:
                            logger.error(f"Failed to execute '{command_name}' on {vendor} {software} @ ({management_device}) | ")
                            logger.error(f"{json.dumps(data, indent=4)}")
                    else:                    
                        logger.error(f"Failed to execute '{command_name}' on {vendor} {software} @ ({management_device}) | ")

                elif method == 'POST':
                    code, data = api.post(endpoint, payload=payload)
                    if code == 409:  # Conflict - likely means the syslog server settings already exist, so we'll treat that as a success for our purposes since our goal is just to ensure the settings are in place for the audit tests
                        logger.info(f"Successfully executed '{command_name}' on {vendor} {software} @ ({management_device}) | ")                       
                    else:
                        if code in [200, 201, 204]:
                            logger.info(f"Successfully executed '{command_name}' on {vendor} {software} @ ({management_device}) | ")
                        else:
                            logger.error(f"Failed to execute '{command_name}' on {vendor} {software} @ ({management_device}) | ")
                            logger.error(f"{code}, {json.dumps(data, indent=4)}")

                elif method == 'DELETE':
                    
                    code = api.delete(endpoint)
                    if code:  # Not Found - likely means there were no settings to delete, so we'll treat that as a success for our purposes since our goal is just to ensure the settings are removed for cleanup after the tests
                        logger.info(f"Successfully executed '{command_name}' on {vendor} {software} @ ({management_device}) | {code}")
                    else:
                        logger.error(f"Failed to execute '{command_name}' on {vendor} {software} @ ({management_device}) | {code}")
                        
                else:
                    logger.error(f"Failed to execute '{command_name}' on {vendor} {software} @ ({management_device}) | ")
                    logger.error(f"{code}") 
                    
        return True



    # text_logger.log_metadata("Test Suite", "Audit Logging Validation")
    # text_logger.log_metadata("Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # text_logger.log_metadata("Operating System", f"{os.uname().sysname} {os.uname().release}")
    # text_logger.log_metadata("Python Version", sys.version.split()[0])

    





def generate_netapp_audit_events(logger, storage_appliance):
    logger.info("")
    logger.info(f'{equal_80}')
    logger.info("AUDIT LOG GENERATION ACTIONS")
    logger.info(f'{equal_80}')
    logger.info("")
    vendor = storage_appliance['vendor']
    software = storage_appliance['software']
    management_device = storage_appliance['management_device']
    management_username = storage_appliance['management_username']
    management_password = storage_appliance['management_password']
    
    
    # Useful when you need to hold the connection across multiple calls
    valid_admin_commands = [
        'version',
        'set diag',
        'system timeout modify 0',
        'set -showseparator "|"; security login role show -fields vserver,role,cmddirname,access,query',
        'security login role create -role audit_test_role -cmddirname "DEFAULT" -access all',
        'system timeout modify 10',
        'security login role delete -role audit_test_role -cmddirname *',
      
    ]

    ''' These are authenticated valid command attempts via SSH which should generate successful login events, other events for changes, in the audit logs '''
    ssh = SSHClient(host=management_device, username=management_username, password=management_password)
    if ssh.connect():
        for command in valid_admin_commands:
            rc, out, err = ssh.run(command)
            if rc == 0:
                logger.info(f"Successfully connected to {vendor} {software} @ ({management_device}) via SSH to generate audit events | Command: {command}")
                # for line in out.splitlines():
                #     logger.info(f"Command output: {line.strip()}")
                # logger.info("---")
            else:
                logger.error(f"Failed to execute command on {vendor} {software} @ ({management_device}) via SSH to generate audit events | Command: {command} | Error: {err.strip()}")

       
        ssh.disconnect()
    else:
        logger.error(f"Failed to connect to {vendor} {software} @ ({management_device}) via SSH to generate audit events | Error: {ssh.connect()}")

    return True









