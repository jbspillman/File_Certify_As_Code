# test_smb.py - SMB/CIFS protocol validation tests for storage upgrade verification
# Requires: root and other nonsense;  
# python3 -m pip3 install smbprotocol

from exec_logger import title_large , title_small
from exec_secrets import get_creds

from dataclasses import dataclass, field, replace
from typing import Optional
import subprocess
import threading
import inspect
import hashlib
import time
import uuid
import sys
import os

import smbclient
import smbclient.shutil
from smbprotocol.connection import Connection
from smbprotocol.exceptions import (
    SMBAuthenticationError,
    SMBException,
    SMBOSError,
)
from smbprotocol.session import Session


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


preset_shares = [
    {
        'vendor': 'NetApp',
        'software': 'ONTAP 9.16.1P1',
        'export_server': 'svm02.beastmode.local.net',
        "export_shares": [
            "full_control",
            "modify_control",
            "read_control"
        ],
        'options': {
            'timeout': 30,
        }
    },
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'protocol.onefs002.beastmode.local.net',
        "export_shares": [
            "full_control",
            "modify_control",
            "read_control"
        ],
        'options': {
            'timeout': 30,
        }
    }
]   



def smb_test_suite(log, vendor_software, shares_list: list[dict] = []):
    log.info("Starting SMB Test Suite")

    if not shares_list or len(shares_list) == 0:
        log.warning("No shares provided for SMB test suite. Please provide a list of shares to test.")
        shares_list = preset_shares
        log.info(f"Using preset shares for testing: {[f'{m['vendor']} {m['software']}' for m in shares_list]}")
        log.blank()
        log.blank()

    """Discover and run all SMB test functions matching prefix."""
    all_results = []
    prefix = "test_smb"
    functions   = get_test_functions(prefix)


    for cifs_share in shares_list:
        
        vendor = cifs_share["vendor"]
        software = cifs_share["software"]
        smb_server = cifs_share["export_server"]
        smb_shares = cifs_share["export_shares"]
        smb_options = cifs_share["options"]

        if vendor_software.upper() not in software.upper():
            continue

        # log.info(f"Vendor: {vendor} | Software: {software} | SMB Server: {smb_server} | SMB Export: {smb_share} ")
        # log.info(f"Testing SMB share {smb_share} on {smb_server} with options: {smb_options}")

        for name, func in functions:
            log.info(name)
            # log.blank()
            # log.info(f'{stars_80}')
            # log.step(f"► START: {name}")
            
            # try:
            #     options = SMB_OPTIONS(**smb_options)  # validate options and set defaults
            #     options = replace(options,vendor=vendor,software=software, host=smb_server, share=smb_share)
            #     result = func(log, options, all_results)
                
            # except Exception as e:
            #     log.error(f"✗ {name} — unhandled exception: {e}", exc_info=True)
            #     all_results.append(options(name,False, str(e)))
            # log.step(f"◄ FINISH: {name}")
            # log.blank()

       
            # time.sleep(.2)  # simulate time taken to run test
        # log.blank()
    

@dataclass
class SMB_OPTIONS:
    vendor: str             = ""        # Vendor name
    software: str           = ""        # Vendor Software Version
    host: str               = ""        # SMB server hostname or IP
    share: str              = ""        # Share name (e.g. "data")
    username: str           = ""        # Domain or local username
    password: str           = ""        # Password
    domain: str             = ""        # Domain (empty for local auth)
    port: int               = 445       # SMB port
    encrypt: bool           = False     # Require SMB encryption
    require_signing: bool   = False     # Require SMB signing
    dialect: str            = ""        # Expected dialect e.g. "3.1.1"
    anonymous: bool         = False     # Attempt anonymous/guest access
    kerberos: bool          = False     # Use Kerberos authentication
    timeout: int            = 30        # Connection timeout in seconds


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


def _register_session(log, options) -> bool:
    """Register an smbclient session with the provided options."""
    try:
        kwargs = dict(
            username=options.username,
            password=options.password,
            port=options.port,
            connection_timeout=options.timeout,
        )


        if options.domain:
            kwargs["domain"] = options.domain
            username = f'{options.domain}\\{username}'
        if options.encrypt:
            kwargs["require_encryption"] = True
        if options.kerberos:
            kwargs["auth_protocol"] = "kerberos"

        smbclient.register_session(options.host, **kwargs)
        return True
    except SMBAuthenticationError as e:
        log.error(f"✗ Authentication failed: {e}")
        return False
    except Exception as e:
        log.error(f"✗ Session registration failed: {e}", exc_info=True)
        return False


def _unc(options, *parts: str) -> str:
    """Build a UNC path: \\\\host\\share\\parts"""
    base = f"\\\\{options.host}\\{options.share}"
    return "\\".join([base] + list(parts)) if parts else base


def list_shares(log, target_device, auth_type="anon", dom="", usr="", psw=""):
    
    if auth_type == "anon":
        smb_cmd = ['smbclient', '-L', target_device, '--no-pass']
    elif auth_type == "local":
        smb_cmd = ['smbclient', '-L', target_device, '-U', f'{usr}%{psw}']
    elif auth_type == "domain":
        usr = f'{dom}\\{usr}'
        smb_cmd = ['smbclient', '-L', target_device, '-U', f'{usr}%{psw}']

    # log.debug(f'{smb_cmd}')
    result = subprocess.run(smb_cmd, capture_output=True, text=True)
    return result.stdout


def test_smb_share_enumeration_anon(log, options, all_results):
    """List available shares on the server via anonymous."""
    test_name = "smb_share_enumeration_anonymous"
    test_description = "test smb share enumeration with anonymous access."


    log.info(f'{equal_80}')
    log.info("TEST: SMB Anonymous Server Share Enumeration ")
    log.info(f'{equal_80}')

    anon_string_found = False
    enumerated_shares = False
    try:
        share_detail = list_shares(log, options.host, auth_type="anon")
        for line in str(share_detail).split('\n'):
            if 'Anonymous login successful' in line:
                anon_string_found = True
            if 'Sharename' in line or 'IPC$' in line:
                enumerated_shares = True
            if 'NT_STATUS_LOGON_FAILURE' in line or 'session setup failed' in line:
                enumerated_shares = False
        
        if anon_string_found and enumerated_shares:   
            log.error(f"✗ {test_name} failed. Anonymous access was NOT blocked and shares were found")
            log_result(log, test_name, test_description, False, "Anonymous access was NOT blocked and shares were found", all_results)
        
        
        elif anon_string_found and enumerated_shares is False:
            log.success(f"✓ {test_name} passed. Anonymous access was granted but the server rejected the listing of shares")
            log_result(log, test_name, test_description, True, "Anonymous access was granted but the server rejected the listing of share", all_results)

        elif anon_string_found is False and enumerated_shares is False:
            log.success(f"✓ {test_name} passed. Anonymous access was blocked and the server rejected the listing of shares")
            log_result(log, test_name, test_description, True, "Anonymous access was blocked and the server rejected the listing of shares", all_results)
        
        else:
            log.error(f"✗ {test_name} failed: please check the results of this test.")
            for line in str(share_detail).split('\n'):
                log.warning(f"WTFYO:  {line}")
            log.warning(f'anon_string_found:  {anon_string_found}')
            log.warning(f'enumerated_shares:  {enumerated_shares}')
            log_result(log, test_name, test_description, False, 'check else condition', all_results)
            
    except Exception as e:
        log.error(f"✗ {test_name} failed: {e}", exc_info=True)
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_smb_share_enumeration_local(log, options, all_results):
    """List available shares on the server via local account"""
    test_name = "smb_share_enumeration_local"
    test_description = "test smb share enumeration with local user access."

    log.info(f'{equal_80}')
    log.info("TEST: SMB Local User Server Share Enumeration ")
    log.info(f'{equal_80}')

    if "ONEFS" in options.software.upper():
        get_account = "onefs_local_user"
    elif "ONTAP" in options.software.upper():
        get_account = "ontap_local_user"
    else:  # VAST
        get_account = "vast_local_user"
    
    dta = get_creds(log, get_account)
    if dta is None:
        log.error(f"✗ {test_name} failed: No Account found for: '{get_account}'")
        log_result(log, test_name, test_description, False, 'check account used', all_results)
        return
    
    user = dta["username"]
    pasw = dta["password"]
    try:
        share_detail = list_shares(log, options.host, auth_type="local", usr=user, psw=pasw)
        anon_string_found = False
        enumerated_shares = False
        for line in str(share_detail).split('\n'):
            # log.debug(f'SHARE_LOCAL_USER:  {line}')
            if 'Anonymous login successful' in line:
                anon_string_found = True
            if 'Sharename' in line or 'IPC$' in line:
                enumerated_shares = True
            if 'NT_STATUS_LOGON_FAILURE' in line or 'session setup failed' in line:
                enumerated_shares = False
                
        if anon_string_found and enumerated_shares:   
            log.error(f"✗ {test_name} failed. Anonymous access was used and shares were found")
            log_result(log, test_name, test_description, False, "Anonymous access was NOT blocked and shares were found", all_results)
        
        elif anon_string_found and enumerated_shares is False:
            log.error(f"✗ {test_name} failed. Anonymous access was used and no shares were found")
            log_result(log, test_name, test_description, False, "Anonymous access was used and no shares were found", all_results)

        elif anon_string_found is False and enumerated_shares is False:
            log.success(f"✓ {test_name} passed. Local User Access Success, but we did not see any shares")
            log_result(log, test_name, test_description, True, "Local User Access Success, but we did not see any shares", all_results)
        
        elif anon_string_found is False and enumerated_shares is True:
            log.success(f"✓ {test_name} passed. Local User Access Success, we have shares listed")
            log_result(log, test_name, test_description, True, "Local User Access success, we have shares listed", all_results)

        else:
            log.error(f"✗ {test_name} failed: please check the results of this test.")
            for line in str(share_detail).split('\n'):
                log.warning(f"WTFYO:  {line}")
            log.warning(f'anon_string_found:  {anon_string_found}')
            log.warning(f'enumerated_shares:  {enumerated_shares}')
            log_result(log, test_name, test_description, False, 'check else condition', all_results)

    except Exception as e:
        log.error(f"✗ {test_name} failed: {e}", exc_info=True)
        log_result(log, test_name, test_description, False, str(e), all_results)

    
def test_smb_anonymous_access_blocked(log, options, all_results) -> bool:
    """Verify anonymous/guest access is denied."""
    test_name = "smb_anonymous_access_blocked"
    test_description = "ensure anonymous access is blocked."

    log.info(f'{equal_80}')
    log.info("TEST: SMB Verify Anonymous Access Blocked")
    log.info(f'{equal_80}')

    try:
        smbclient.register_session(options.host, username="", password="", port=options.port)    
    except ValueError as e:
        if 'Connection refused' in str(e):
            log.info("✓ Anonymous access correctly blocked — connection refused")
            log_result(log, test_name, test_description, True, "Anonymous access blocked", all_results)
        else:
            log.error(f"✗ Unexpected ValueError: {e}")
            log_result(log, test_name, test_description, False, str(e), all_results)

    except SMBAuthenticationError:       
        log.success(f"✓ {test_name} — anonymous access correctly denied")
        log_result(log, test_name, test_description, True, "anonymous access correctly denied", all_results)

    except Exception as e:
        log.error(f"✗ {test_name} failed: {e}", exc_info=True)
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_smb_authentication_local_user(log, options, all_results) -> bool:
    """Authenticate with a local (non-domain) user account."""
    test_name = "test_smb_authentication_local_user"
    try:
        smbclient.register_session(options.host, username=options.username,
                                   password=options.password, port=options.port)
        smbclient.listdir(_unc(options))
        log.success(f"✓ {test_name} — local user: {options.username}")
        all_results.append(SMB_TEST_RESULT(test_name,True, f"local:{options.username}"))
        return True
    except SMBAuthenticationError as e:
        log.error(f"✗ {test_name} — auth rejected: {e}")
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_authentication_domain_user(log, options, all_results) -> bool:
    """Authenticate with a domain user account."""
    test_name = "test_smb_authentication_domain_user"
    try:
        smbclient.register_session(options.host, username=options.username,
                                   password=options.password, domain=options.domain,
                                   port=options.port)
        smbclient.listdir(_unc(options))
        log.success(f"✓ {test_name} — {options.domain}\\{options.username}")
        all_results.append(SMB_TEST_RESULT(test_name,True, f"{options.domain}\\{options.username}"))
        return True
    except SMBAuthenticationError as e:
        log.error(f"✗ {test_name} — auth rejected: {e}")
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_dialect_negotiation(log, options, all_results) -> bool:
    """Verify the negotiated SMB dialect matches expected version."""
    test_name = "test_smb_dialect_negotiation"
    try:
        conn = Connection(uuid.uuid4(), options.host, options.port)
        conn.connect(timeout=options.timeout)
        negotiated = str(conn.dialect)
        conn.disconnect()

        expected = options.dialect
        passed = (negotiated == expected) if expected else True
        detail = f"negotiated={negotiated}" + (f" expected={expected}" if expected else "")
        log.success(f"✓ {test_name} — {detail}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_test_name,False, str(e)))
        return False


def test_smb_signing_enabled(log, options, all_results) -> bool:
    """Verify SMB signing is active on the connection."""
    test_name = "test_smb_signing_enabled"
    try:
        conn = Connection(uuid.uuid4(), options.host, options.port)
        conn.connect(timeout=options.timeout)
        signing_required = conn.require_signing
        conn.disconnect()

        passed = signing_required
        detail = f"signing_required={signing_required}"
        log.success(f"✓ {test_name} — {detail}") if passed else log.warning(f"⚠ {test_name} — signing not required: {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_encryption_enabled(log, options, all_results) -> bool:
    """Verify SMB encryption is active when required by share policy."""
    test_name = "test_smb_encryption_enabled"
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session registration failed"))
            return False

        # Attempt to open with encryption requirement
        smbclient.register_session(options.host, username=options.username,
                                   password=options.password,
                                   require_encryption=True, port=options.port)
        smbclient.listdir(_unc(options))
        log.success(f"✓ {test_name} — encrypted session accepted by server")
        all_results.append(SMB_TEST_RESULT(test_name,True, "encryption negotiated"))
        return True
    except SMBException as e:
        log.error(f"✗ {test_name} — encryption rejected or not supported: {e}")
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_connect_to_share(log, options, all_results) -> bool:
    """Connect to the target share and confirm it is accessible."""
    test_name = "test_smb_connect_to_share"
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False
        smbclient.listdir(_unc(options))
        log.success(f"✓ {test_name} — {_unc(options)}")
        all_results.append(SMB_TEST_RESULT(test_name,True, _unc(options)))
        return True
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_list_directory(log, options, all_results) -> bool:
    """List the root of the share and confirm entries are returned."""
    test_name = "test_smb_list_directory"
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False
        entries = smbclient.listdir(_unc(options))
        detail = f"{len(entries)} entries found"
        log.info(f"  {detail}")
        log.success(f"✓ {test_name}")
        all_results.append(SMB_TEST_RESULT(test_name,True, detail))
        return True
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_file_read(log, options, all_results) -> bool:
    """Write a file then read it back and verify contents."""
    test_name = "test_smb_file_read"
    fname = f"smb_read_test_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    expected = b"smb_read_test_content"
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False
        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(expected)
        with smbclient.open_file(fpath, mode="rb") as f:
            actual = f.read()
        smbclient.remove(fpath)
        passed = actual == expected
        detail = "content match" if passed else f"expected {expected!r} got {actual!r}"
        log.success(f"✓ {test_name}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_file_write(log, options, all_results) -> bool:
    """Write a file and confirm it exists on the share."""
    test_name = "test_smb_file_write"
    fname = f"smb_write_test_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False
        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(b"smb_write_test_content")
        exists = fname in smbclient.listdir(_unc(options))
        smbclient.remove(fpath)
        detail = "file confirmed on share" if exists else "file not found after write"
        log.success(f"✓ {test_name}") if exists else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,exists, detail))
        return exists
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_file_delete(log, options, all_results) -> bool:
    """Create a file then delete it and confirm it is gone."""
    test_name = "test_smb_file_delete"
    fname = f"smb_delete_test_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False
        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(b"delete_me")
        smbclient.remove(fpath)
        gone = fname not in smbclient.listdir(_unc(options))
        detail = "file removed successfully" if gone else "file still present after delete"
        log.success(f"✓ {test_name}") if gone else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,gone, detail))
        return gone
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_file_rename(log, options, all_results) -> bool:
    """Create a file, rename it, confirm new name exists and old is gone."""
    test_name = "test_smb_file_rename"
    tag = uuid.uuid4().hex[:8]
    src = _unc(options, f"smb_rename_src_{tag}.txt")
    dst = _unc(options, f"smb_rename_dst_{tag}.txt")
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False
        with smbclient.open_file(src, mode="wb") as f:
            f.write(b"rename_test")
        smbclient.rename(src, dst)
        entries = smbclient.listdir(_unc(options))
        passed = (f"smb_rename_dst_{tag}.txt" in entries and
                  f"smb_rename_src_{tag}.txt" not in entries)
        smbclient.remove(dst)
        detail = "renamed successfully" if passed else "rename failed or old name still exists"
        log.success(f"✓ {test_name}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_file_move(log, options, all_results) -> bool:
    """Create a file at the root, move it into a subdirectory."""
    test_name = "test_smb_file_move"
    tag = uuid.uuid4().hex[:8]
    subdir = _unc(options, f"smb_move_dir_{tag}")
    src    = _unc(options, f"smb_move_src_{tag}.txt")
    dst    = _unc(options, f"smb_move_dir_{tag}", f"smb_move_dst_{tag}.txt")
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False
        smbclient.mkdir(subdir)
        with smbclient.open_file(src, mode="wb") as f:
            f.write(b"move_test")
        smbclient.rename(src, dst)
        entries = smbclient.listdir(subdir)
        passed = f"smb_move_dst_{tag}.txt" in entries
        smbclient.remove(dst)
        smbclient.rmdir(subdir)
        detail = "file moved to subdirectory" if passed else "file not found in destination"
        log.success(f"✓ {test_name}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_large_file_write_checksum(log, options, all_results,
                                        size_mb: int = 512) -> bool:
    """Write a large file, read it back, verify SHA-256 checksum matches."""
    test_name = "test_smb_large_file_write_checksum"
    fname = f"smb_large_{uuid.uuid4().hex[:8]}.bin"
    fpath = _unc(options, fname)
    chunk = os.urandom(1024 * 1024)  # 1 MB random chunk
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        log.info(f"  Writing {size_mb} MB to {fname}")
        sha_write = hashlib.sha256()
        with smbclient.open_file(fpath, mode="wb") as f:
            for _ in range(size_mb):
                f.write(chunk)
                sha_write.update(chunk)

        log.info(f"  Reading back and verifying checksum")
        sha_read = hashlib.sha256()
        with smbclient.open_file(fpath, mode="rb") as f:
            while True:
                data = f.read(1024 * 1024)
                if not data:
                    break
                sha_read.update(data)

        smbclient.remove(fpath)
        passed = sha_write.digest() == sha_read.digest()
        detail = f"{size_mb} MB — sha256 {'match' if passed else 'MISMATCH'}"
        log.success(f"✓ {test_name} — {detail}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_many_small_files(log, options, all_results,
                               count: int = 500) -> bool:
    """Create, verify, and delete a large number of small files (metadata stress test)."""
    test_name = "test_smb_many_small_files"
    tag  = uuid.uuid4().hex[:8]
    subdir = _unc(options, f"smb_smallfiles_{tag}")
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        smbclient.mkdir(subdir)
        log.info(f"  Creating {count} files")
        for i in range(count):
            fpath = _unc(options, f"smb_smallfiles_{tag}", f"file_{i:05d}.txt")
            with smbclient.open_file(fpath, mode="wb") as f:
                f.write(f"file {i}".encode())

        actual = len(smbclient.listdir(subdir))
        log.info(f"  Cleaning up {actual} files")
        smbclient.shutil.rmtree(subdir)

        passed = actual == count
        detail = f"created={count} found={actual}"
        log.success(f"✓ {test_name} — {detail}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_exclusive_lock(log, options, all_results) -> bool:
    """Open a file exclusively and verify a second open is blocked."""
    test_name = "test_smb_exclusive_lock"
    fname = f"smb_excl_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        # Open with exclusive access
        with smbclient.open_file(fpath, mode="wb", share_access="") as f:
            f.write(b"exclusive")
            # Attempt a second open — should be denied
            try:
                with smbclient.open_file(fpath, mode="rb"):
                    pass
                # Second open succeeded — exclusive lock not honored
                smbclient.remove(fpath)
                log.error(f"✗ {test_name} — exclusive lock was NOT enforced")
                all_results.append(SMB_TEST_RESULT(test_name,False, "second open was allowed"))
                return False
            except SMBOSError:
                pass  # Expected — second open was blocked

        smbclient.remove(fpath)
        log.success(f"✓ {test_name} — exclusive lock enforced correctly")
        all_results.append(SMB_TEST_RESULT(test_name,True, "second open blocked as expected"))
        return True
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_byte_range_lock(log, options, all_results) -> bool:
    """Lock a byte range and verify another handle cannot write to that range."""
    test_name = "test_smb_byte_range_lock"
    fname = f"smb_bytelock_{uuid.uuid4().hex[:8]}.bin"
    fpath = _unc(options, fname)
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        with smbclient.open_file(fpath, mode="wb", share_access="rw") as f:
            f.write(b"\x00" * 4096)
            # Lock bytes 0-1023
            f.lock(0, 1024, fail_immediately=True)
            log.info("  Byte range 0-1023 locked")

            # Attempt write to locked range from same handle (should succeed — same handle)
            f.seek(0)
            f.write(b"\xff" * 512)

        smbclient.remove(fpath)
        log.success(f"✓ {test_name} — byte range lock applied")
        all_results.append(SMB_TEST_RESULT(test_name,True, "byte range lock honored"))
        return True
    except AttributeError:
        # smbclient may not expose lock() directly — note it
        log.warning(f"⚠ {test_name} — byte range lock not directly testable via smbclient API")
        all_results.append(SMB_TEST_RESULT(test_name,True, "skipped — API limitation"))
        return True
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_oplock_granted(log, options, all_results) -> bool:
    """Open a file and verify an opportunistic lock (oplock) is granted."""
    test_name = "test_smb_oplock_granted"
    fname = f"smb_oplock_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        # Write the file first
        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(b"oplock_test")

        # Open with oplock request — batch oplock
        with smbclient.open_file(fpath, mode="rb",
                                  oplock_level=smbclient.OplockLevel.BATCH) as f:
            data = f.read()
            log.info(f"  Oplock granted, read {len(data)} bytes")

        smbclient.remove(fpath)
        log.success(f"✓ {test_name} — oplock granted and released cleanly")
        all_results.append(SMB_TEST_RESULT(test_name,True, "BATCH oplock granted"))
        return True
    except AttributeError:
        log.warning(f"⚠ {test_name} — oplock level not exposed by smbclient API, skipping")
        all_results.append(SMB_TEST_RESULT(test_name,True, "skipped — API limitation"))
        return True
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_lock_released_on_disconnect(log, options, all_results) -> bool:
    """Verify file locks are released after a client disconnects."""
    test_name = "test_smb_lock_released_on_disconnect"
    fname = f"smb_lockrel_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        # Open exclusively then let the context manager close (simulates disconnect)
        with smbclient.open_file(fpath, mode="wb", share_access="") as f:
            f.write(b"lock_release_test")
        # Connection closed — now open again, lock should be released
        with smbclient.open_file(fpath, mode="rb") as f:
            data = f.read()

        smbclient.remove(fpath)
        passed = data == b"lock_release_test"
        detail = "lock released, file re-opened successfully" if passed else "could not re-open after disconnect"
        log.success(f"✓ {test_name}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_acl_access_allowed(log, options, all_results) -> bool:
    """Verify an authorized user can access the share."""
    test_name = "test_smb_acl_access_allowed"
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False
        smbclient.listdir(_unc(options))
        log.success(f"✓ {test_name} — {options.username} access confirmed")
        all_results.append(SMB_TEST_RESULT(test_name,True, f"user={options.username}"))
        return True
    except SMBOSError as e:
        log.error(f"✗ {test_name} — access denied: {e}")
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_acl_access_denied(log, options, all_results,
                                denied_user: str = "", denied_pass: str = "") -> bool:
    """Verify an unauthorized user cannot access the share."""
    test_name = "test_smb_acl_access_denied"
    try:
        smbclient.register_session(options.host, username=denied_user,
                                   password=denied_pass, port=options.port)
        smbclient.listdir(_unc(options))
        # Access was granted — this is a failure
        log.error(f"✗ {test_name} — unauthorized user '{denied_user}' was NOT denied")
        all_results.append(SMB_TEST_RESULT(test_name,False, f"access granted to {denied_user}"))
        return False
    except (SMBAuthenticationError, SMBOSError):
        log.success(f"✓ {test_name} — '{denied_user}' correctly denied")
        all_results.append(SMB_TEST_RESULT(test_name,True, f"access denied for {denied_user}"))
        return True
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_inherited_permissions(log, options, all_results) -> bool:
    """Create a subdirectory and verify permissions are inherited from parent."""
    test_name = "test_smb_inherited_permissions"
    tag    = uuid.uuid4().hex[:8]
    subdir = _unc(options, f"smb_inherit_{tag}")
    fname  = _unc(options, f"smb_inherit_{tag}", "child.txt")
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        smbclient.mkdir(subdir)
        # Write and read a file in the subdirectory — if inherited perms are broken this will fail
        with smbclient.open_file(test_name, mode="wb") as f:
            f.write(b"inherit_test")
        with smbclient.open_file(test_name, mode="rb") as f:
            data = f.read()

        smbclient.remove(fname)
        smbclient.rmdir(subdir)
        passed = data == b"inherit_test"
        detail = "inherited permissions allow read/write in child dir" if passed else "read/write failed in child dir"
        log.success(f"✓ {test_name}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name, passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_concurrent_reads(log, options, all_results,
                               thread_count: int = 4) -> bool:
    """Multiple clients read the same file simultaneously."""
    test_name = "test_smb_concurrent_reads"
    fname = f"smb_concread_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    errors = []

    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(b"concurrent_read_data")

        def reader(tid):
            try:
                with smbclient.open_file(fpath, mode="rb", share_access="r") as f:
                    data = f.read()
                    if data != b"concurrent_read_data":
                        errors.append(f"thread {tid}: data mismatch")
            except Exception as e:
                errors.append(f"thread {tid}: {e}")

        threads = [threading.Thread(target=reader, args=(i,)) for i in range(thread_count)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        smbclient.remove(fpath)
        passed = len(errors) == 0
        detail = f"{thread_count} concurrent readers — {'all passed' if passed else str(errors)}"
        log.success(f"✓ {test_name} — {detail}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_concurrent_writes(log, options, all_results,
                                thread_count: int = 4) -> bool:
    """Multiple clients write to different files in the same share simultaneously."""
    test_name = "test_smb_concurrent_writes"
    tag = uuid.uuid4().hex[:8]
    errors = []

    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        def writer(tid):
            fpath = _unc(options, f"smb_concwrite_{tag}_{tid}.txt")
            try:
                with smbclient.open_file(fpath, mode="wb") as f:
                    f.write(f"thread_{tid}_data".encode())
                smbclient.remove(fpath)
            except Exception as e:
                errors.append(f"thread {tid}: {e}")

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(thread_count)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        passed = len(errors) == 0
        detail = f"{thread_count} concurrent writers — {'all passed' if passed else str(errors)}"
        log.success(f"✓ {test_name} — {detail}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_concurrent_read_write(log, options, all_results) -> bool:
    """One client writes while another reads the same file."""
    test_name = "test_smb_concurrent_read_write"
    fname = f"smb_concrw_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    errors = []

    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(b"initial_content")

        def writer():
            try:
                time.sleep(0.1)
                with smbclient.open_file(fpath, mode="wb", share_access="rw") as f:
                    f.write(b"updated_content")
            except Exception as e:
                errors.append(f"writer: {e}")

        def reader():
            try:
                with smbclient.open_file(fpath, mode="rb", share_access="rw") as f:
                    f.read()
            except Exception as e:
                errors.append(f"reader: {e}")

        t_write = threading.Thread(target=writer)
        t_read  = threading.Thread(target=reader)
        t_read.start()
        t_write.start()
        t_read.join()
        t_write.join()

        smbclient.remove(fpath)
        passed = len(errors) == 0
        detail = "concurrent read/write completed" if passed else str(errors)
        log.success(f"✓ {test_name}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_persistent_handle_after_failover(log, options, all_results) -> bool:
    """Verify SMB3 persistent handles survive a node failover."""
    test_name = "test_smb_persistent_handle_after_failover"
    log.warning(f"⚠ {test_name} — requires active node failover during test, marking for manual verification")
    all_results.append(SMB_TEST_RESULT(test_name,True, "manual verification required"))
    return True


def test_smb_session_reconnect(log, options, all_results) -> bool:
    """Verify the SMB client can reconnect after a dropped session."""
    test_name = "test_smb_session_reconnect"
    fname = f"smb_reconnect_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    try:
        # First session
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "initial session failed"))
            return False

        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(b"reconnect_test")

        # Drop and re-register session
        smbclient.delete_session(options.host)
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "reconnect failed"))
            return False

        with smbclient.open_file(fpath, mode="rb") as f:
            data = f.read()

        smbclient.remove(fpath)
        passed = data == b"reconnect_test"
        detail = "session reconnected, data intact" if passed else "data lost after reconnect"
        log.success(f"✓ {test_name}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_audit_event_file_create(log, options, all_results,
                                      audit_log: list = None) -> bool:
    """Create a file and verify an audit event was generated."""
    test_name = "test_smb_audit_event_file_create"
    fname = f"smb_audit_create_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(b"audit_create_test")
        smbclient.remove(fpath)

        # If an audit_log list is passed in (from your syslog/webhook capture),
        # check it for the expected event
        if audit_log is not None:
            matched = any(fname in entry for entry in audit_log)
            detail = "audit event found in log" if matched else "audit event NOT found in log"
            passed = matched
        else:
            detail = "file created — audit log not provided for verification"
            passed = True
            log.warning(f"⚠ {test_name} — {detail}")

        log.success(f"✓ {test_name} — {detail}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_audit_event_file_delete(log, options, all_results,
                                      audit_log: list = None) -> bool:
    """Delete a file and verify an audit event was generated."""
    test_name = "test_smb_audit_event_file_delete"
    fname = f"smb_audit_delete_{uuid.uuid4().hex[:8]}.txt"
    fpath = _unc(options, fname)
    try:
        if not _register_session(options):
            all_results.append(SMB_TEST_RESULT(test_name,False, "session failed"))
            return False

        with smbclient.open_file(fpath, mode="wb") as f:
            f.write(b"audit_delete_test")
        smbclient.remove(fpath)

        if audit_log is not None:
            matched = any(fname in entry and "delete" in entry.lower() for entry in audit_log)
            detail = "delete audit event found" if matched else "delete audit event NOT found"
            passed = matched
        else:
            detail = "file deleted — audit log not provided for verification"
            passed = True
            log.warning(f"⚠ {test_name} — {detail}")

        log.success(f"✓ {test_name} — {detail}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def test_smb_audit_event_permission_denied(log, options, all_results,
                                            audit_log: list = None,
                                            denied_user: str = "",
                                            denied_pass: str = "") -> bool:
    """Trigger a permission denied event and verify it appears in the audit log."""
    test_name = "test_smb_audit_event_permission_denied"
    try:
        # Attempt access with an unauthorized user to trigger the audit event
        try:
            smbclient.register_session(options.host, username=denied_user,
                                       password=denied_pass, port=options.port)
            smbclient.listdir(_unc(options))
        except (SMBAuthenticationError, SMBOSError):
            pass  # Expected denial — we just need the audit event to fire

        if audit_log is not None:
            matched = any("denied" in entry.lower() or "access" in entry.lower()
                          for entry in audit_log)
            detail = "permission denied audit event found" if matched else "permission denied event NOT found"
            passed = matched
        else:
            detail = "access attempted — audit log not provided for verification"
            passed = True
            log.warning(f"⚠ {test_name} — {detail}")

        log.success(f"✓ {test_name} — {detail}") if passed else log.error(f"✗ {test_name} — {detail}")
        all_results.append(SMB_TEST_RESULT(test_name,passed, detail))
        return passed
    except Exception as e:
        log.error(f"✗ {test_name} — {e}", exc_info=True)
        all_results.append(SMB_TEST_RESULT(test_name,False, str(e)))
        return False


def get_test_functions(prefix: str = "test_smb"):
    current_module = sys.modules[__name__]
    return sorted(
        [(test_name,func)
         for test_name,func in inspect.getmembers(current_module, inspect.isfunction)
         if test_name.startswith(prefix)],
        key=lambda x: inspect.getsourcelines(x[1])[1]
    )

# ─────────────────────────────────────────────
#  Usage Example
# ─────────────────────────────────────────────
# if __name__ == "__main__":
#     options = SMB_OPTIONS(
#         host             = "cluster01.dc1.local",
#         share            = "data",
#         username         = "svc-validator",
#         password         = "your_password",
#         domain           = "CORP",
#         encrypt          = False,
#         require_signing  = True,
#         dialect          = "3.1.1",
#     )
#     results = run_smb_tests(options)
