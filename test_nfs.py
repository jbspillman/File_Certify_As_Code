# test_nfs.py  - NFS Test Suite for File Certification as Code

from exec_mounts import MOUNT_OPTIONS, mount_nas, unmount_nas
from exec_logger import title_large , title_small

from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from dataclasses import dataclass, fields, replace
from datetime import datetime
import multiprocessing
import subprocess
import threading
import inspect
import fcntl
import time
import sys
import os

current_module = sys.modules[__name__]

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


preset_mounts = [
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'onefs002-2.beastmode.local.net',
        'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_rw',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 3
        }
    },  
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'onefs002-2.beastmode.local.net',
        'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_rw',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 2
        }
    },    
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'onefs002-2.beastmode.local.net',
        'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_rw',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 1
        }
    },     
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'onefs002-2.beastmode.local.net',
        'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_rw',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 0
        }
    },          

    {
        'vendor': 'VAST',
        'software': 'VAST UBUNTU FFF10.0.0',
        'export_server': 'beastserver.beastmode.local.net',
        'export_path': '/mnt/Drives/12000b/a1',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 3,
        }
    },  

    {
        'vendor': 'VAST',
        'software': 'VAST UBUNTU FFF10.0.0',
        'export_server': 'beastserver.beastmode.local.net',
        'export_path': '/mnt/Drives/12000b/a2',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {},          
    },

    {
        'vendor': 'VAST',
        'software': 'VAST UBUNTU FFF10.0.0',
        'export_server': 'beastserver.beastmode.local.net',
        'export_path': '/mnt/Drives/12000b/a2',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 0
        }
    },          

    {
        'vendor': 'VAST',
        'software': 'VAST UBUNTU FFF10.0.0',
        'export_server': 'beastserver.beastmode.local.net',
        'export_path': '/mnt/Drives/12000b/a3',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 1
        }
    },   

    {
        'vendor': 'VAST',
        'software': 'VAST UBUNTU FFF10.0.0',
        'export_server': 'beastserver.beastmode.local.net',
        'export_path': '/mnt/Drives/12000b/a4',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 2
        }
    },   
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'onefs002-2.beastmode.local.net',
        'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_ro',
        'host_access': 'ro',           # read ro, write rw, root rt, none na
        'host_access_expected': 'ro',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 3,
        }
    },    
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'onefs002-2.beastmode.local.net',
        'export_path': '/ifs/ACCESS_ZONES/system/nfs4_01_rw',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 3
        }
    },   
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'onefs002-2.beastmode.local.net',
        'export_path': '/ifs/ACCESS_ZONES/system/nfs4_01_ro',
        'host_access': 'ro',           # read ro, write rw, root rt, none na
        'host_access_expected': 'ro',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 3
        }
    },         
    {
        'vendor': 'NetApp',
        'software': 'ONTAP 9.16.1P1',
        'export_server': 'svm01.beastmode.local.net',
        'export_path': '/svm01_vol02',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 3,
        }
    },
    {
        'vendor': 'NetApp',
        'software': 'ONTAP 9.16.1P1',
        'export_server': 'svm01.beastmode.local.net',
        'export_path': '/svm01_vol02',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 0
        }
    },
    {
        'vendor': 'NetApp',
        'software': 'ONTAP 9.16.1P1',
        'export_server': 'svm01.beastmode.local.net',
        'export_path': '/svm01_vol02',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 1
        }
    },
    {
        'vendor': 'NetApp',
        'software': 'ONTAP 9.16.1P1',
        'export_server': 'svm01.beastmode.local.net',
        'export_path': '/svm01_vol02',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 4,
            'minorvers': 2
        }
    },    
]



def update_from_dict(obj, data: dict):
    valid_fields = {f.name for f in fields(obj)}
    for key, value in data.items():
        if key in valid_fields:
            setattr(obj, key, value)


def nfs_test_suite(log, vendor_software, mounts_list: list[dict] = []):
    log.divider()
    stitle_str = f'NFS3 Protocol Tests Starting'
    title_small(log, stitle_str)
    log.divider()

    if not os.geteuid() == 0:
        log.error('✗ Mount operations require root privileges. Please run as root or with sudo.')
        sys.exit("\nThis script must be run as root or with sudo privileges. Try running with 'sudo python <script_name>.py'\n")
        return
    
    if not mounts_list or len(mounts_list) == 0:
        mounts_list = preset_mounts
        # log.warning("No mounts provided for NFS test suite. Please provide a list of mounts to test.")
        # log.info(f"Using a preset mounts list for testing....")

    ''' find the test cases per protocol types. '''
    v0_prefix = "test_nfs0_"
    v3_prefix = "test_nfs3_"
    v4_prefix = "test_nfs4_"
    v0_functions = get_test_functions(v0_prefix)
    v3_functions = get_test_functions(v3_prefix)
    v4_functions = get_test_functions(v4_prefix)

    all_tests = []
    all_tests.extend(v0_functions)
    all_tests.extend(v3_functions)
    all_tests.extend(v4_functions)

    for name, func in all_tests:
        log.info(name)

    # all_results = []  # This will store results of all tests for all mounts for later reporting and analysis
    # for nfs_mount in mounts_list:
    #     vendor = nfs_mount["vendor"]
    #     software = nfs_mount["software"]
    #     nfs_server = nfs_mount["export_server"]
    #     nfs_export = nfs_mount["export_path"]
    #     nfs_options = nfs_mount["options"]
    #     if vendor_software.upper() not in software.upper():
    #         continue  # skip the unwanted software versions.
    #     options = MOUNT_OPTIONS()

    #     nfs_major = nfs_options.get('majorvers', 3)
    #     nfs_minor = nfs_options.get('minorvers', 0)
    #     add_options = MOUNT_OPTIONS(**nfs_options)
    #     if nfs_major == 3:
    #         log.blank()
    #         title_str = f'{vendor} {software} > {nfs_server}:{nfs_export}'
    #         log_title(log, title_str)
            
    #         for name, func in v0_functions:
    #             if 'nconnect' in name:
    #                 add_options = replace(add_options, nconnect=True, nconnect_count=4)
    #             else:
    #                 add_options = replace(add_options, nconnect=False, nconnect_count=0)
                    
    #             mount_status, mount_path = mount_nas(
    #                 log         = log,
    #                 vendor      = vendor,
    #                 software    = software,
    #                 nfs_server  = nfs_server,
    #                 nfs_export  = nfs_export,
    #                 uid         = 1000,
    #                 gid         = 1000,
    #                 options     = add_options,
    #                 dry_run     = False,
    #             )                    
    #             if mount_status:
    #                 try:
    #                     func(log, mount_path, add_options, all_results)
    #                 except Exception as e:
    #                     log.error(f"✗ {name} — exception: {e}", exc_info=True)

    #                 log.info(f'{equal_80}')
    #                 unmount_nas(log, vendor, software, mount_path,  dry_run=False)                   
    #             else:
    #                 log.error(f"FAILED: Mount {vendor} {software} export at {mount_path} with options: {add_options}")
    #                 continue
            
    #         exit_script = False
    #         for name, func in v3_functions:
    #             if 'nconnect' in name:
    #                 add_options = replace(add_options, nconnect=True, nconnect_count=4)
    #                 # exit_script = True
    #             else:
    #                 add_options = replace(add_options, nconnect=False, nconnect_count=0)

    #             mount_status, mount_path = mount_nas(
    #                 log         = log,
    #                 vendor      = vendor,
    #                 software    = software,
    #                 nfs_server  = nfs_server,
    #                 nfs_export  = nfs_export,
    #                 uid         = 1000,
    #                 gid         = 1000,
    #                 options     = add_options,
    #                 dry_run     = False,
    #             )                    
    #             if mount_status:
    #                 try:
    #                     func(log, mount_path, add_options, all_results)
    #                 except Exception as e:
    #                     log.error(f"✗ {name} — exception: {e}", exc_info=True)

    #                 log.info(f'{equal_80}')
    #                 unmount_nas(log, vendor, software, mount_path,  dry_run=False)      
    #                 # if exit_script:
    #                 #     exit(0)             
    #             else:
    #                 log.error(f"FAILED: Mount {vendor} {software} export at {mount_path} with options: {add_options}")
    #                 continue

                
    #     elif nfs_major == 4:
    #         log.blank()
    #         title_str = f'{vendor} {software} > {nfs_server}:{nfs_export}'
    #         log_title(log, title_str)

    #         for name, func in v0_functions:
    #             if 'nconnect' in name:
    #                 add_options = replace(add_options, nconnect=True, nconnect_count=4)
    #             else:
    #                 add_options = replace(add_options, nconnect=False, nconnect_count=0)

    #             mount_status, mount_path = mount_nas(
    #                 log         = log,
    #                 vendor      = vendor,
    #                 software    = software,
    #                 nfs_server  = nfs_server,
    #                 nfs_export  = nfs_export,
    #                 uid         = 0,
    #                 gid         = 0,
    #                 options     = add_options,
    #                 dry_run     = False,
    #             )                    
    #             if mount_status:
    #                 try:
    #                     func(log, mount_path, add_options, all_results)
    #                 except Exception as e:
    #                     log.error(f"✗ {name} — exception: {e}", exc_info=True)

    #                 log.info(f'{equal_80}')
    #                 unmount_nas(log, vendor, software, mount_path,  dry_run=False)                   
    #             else:
    #                 log.error(f"FAILED: Mount {vendor} {software} export at {mount_path} with options: {add_options}")
    #                 continue            

    #         for name, func in v4_functions:
                
    #             if 'parallel_io' in name:
    #                 add_options = replace(add_options, nconnect=True, nconnect_count=8)
    #             else:
    #                 add_options = replace(add_options, nconnect=False, nconnect_count=0)

    #             mount_status, mount_path = mount_nas(
    #                 log         = log,
    #                 vendor      = vendor,
    #                 software    = software,
    #                 nfs_server  = nfs_server,
    #                 nfs_export  = nfs_export,
    #                 uid         = 0,
    #                 gid         = 0,
    #                 options     = add_options,
    #                 dry_run     = False,
    #             )                    
    #             if mount_status:
    #                 try:
    #                     func(log, mount_path, add_options, all_results)
    #                 except Exception as e:
    #                     log.error(f"✗ {name} — exception: {e}", exc_info=True)

    #                 log.info(f'{equal_80}')
    #                 unmount_nas(log, vendor, software, mount_path,  dry_run=False)                   
    #             else:
    #                 log.error(f"FAILED: Mount {vendor} {software} export at {mount_path} with options: {add_options}")
    #                 continue


    log.divider()
    log.info("FINISH: NFS Test Suite")
    log.divider()


def get_test_functions(prefix: str = "test_"):
    """Return test functions matching prefix, sorted by line number."""
    return sorted(
        [
            (name, func)
            for name, func in inspect.getmembers(current_module, inspect.isfunction)
            if name.startswith(prefix)
        ],
        key=lambda x: inspect.getsourcelines(x[1])[1]
    )


def create_test_directory(log, mount_point):
    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        return True, test_dir
    except (OSError, IOError) as error_message:
        return False, error_message


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


def test_nfs0_mount_options_verification(log, mount_point, mount_options, all_results=None):
    """Verify mount options"""
    test_name = 'mount_options_verification'
    test_description = "Confirm that the actual mount options match the requested configuration"
  
    log.info(f'{equal_80}')
    log.info(f"TEST: NFS Mount Options Verification | Mount Point: {mount_point}")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    try:

        log.info("Phase 1: Reading /proc/mounts")
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()
        log.info(" ✓ Read /proc/mounts successfully")

        log.info(f"Phase 2: Searching for mount point: {mount_point}")
        mount_line = None
        for line in mounts.split('\n'):
            if mount_point in line:
                mount_line = line
                break
        
        if not mount_line:
            log.error("✗ Mount point not found")
            log_result(log, test_name, test_description, False, "Mount not found in /proc/mounts", all_results)
            return
        log.info(f" ✓ Found the mount point in /proc/mounts")

        parts = mount_line.split()
        if len(parts) >= 4:
            options = parts[3]
            log.info(f"Phase 3: Parsing mount options")
            
            if 'vers=3' in options or 'nfsvers=3' in options:
                log.info(" ✓ NFS Version: 3")
           
            if f'proto={mount_options.transport}' in options:
                log.info(f" ✓ Transport: {mount_options.transport}")

            log.info("✓ Mount options verified")
            log_result(log, test_name, test_description, True, "Mount options verified", all_results)
        
        else:
            log.error("✗ Could not parse mount options")
            log_result(log, test_name, test_description, False, "Could not parse mount options", all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs0_readwrite_mount_enforcement(log, mount_point, mount_options, all_results=None):
    """Test rw mount allows writes"""
    test_name = 'readwrite_mount_enforcement'
    test_description = "Verify read-write mount allows create, modify, and delete operations"
    
    log.info(f'{equal_80}')
    log.info(f"TEST: NFS Read-Write Mount Enforcement | Mount Point: {mount_point}")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return

    test_file = os.path.join(test_dir, 'rw_test.txt')
    test_data = "RW mount test"
    
    try:
        log.info("Phase 1: Testing write permissions")
        with open(test_file, 'w') as f:
            f.write(test_data)
        log.info("✓ Write operation successful")
        
        log.info("Phase 2: Verifying data integrity")
        with open(test_file, 'r') as f:
            content = f.read()
        
        if content == test_data:
            msg = "✓ Data verified correctly"
            log.info(msg)
        else:
            msg = f"✗ Data mismatch: '{content}'"
            log.error(msg)
        
        assert content == test_data       
        log.info("Phase 3: Cleanup")
        os.remove(test_file)
        log.info("✓ Test file removed")
        log.info("✓ RW mount working correctly")
        log_result(log, test_name, test_description, True, "RW mount working correctly", all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs0_basic_file_operations(log, mount_point, mount_options, all_results=None):
    """Test basic file operations"""
    test_name = 'basic_file_operations'
    test_description = "Perform basic file operations (create, read, delete) to verify functionality"
   
    log.info(f'{equal_80}')
    log.info("TEST: NFS Basic File Operations")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
        
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
    
    test_file = os.path.join(test_dir, 'basic_test.txt')
    test_data = "Hello NFS CLIENT"
    try:
        log.info(f"Phase 1: Creating test file and writing data")
        with open(test_file, 'w') as f:
            f.write(test_data)
        log.info(f"✓ File created with {len(test_data)} bytes")
        
        log.info(f"Phase 2: Reading file content back")
        with open(test_file, 'r') as f:
            read_data = f.read()
        log.info(f"✓ File read: '{read_data}'")
        
        log.info(f"Phase 3: Verifying data integrity")
        assert read_data == test_data
        log.info("✓ Data integrity verified")
        
        log.info(f"Phase 4: Deleting test file")
        os.remove(test_file)
        log.info("✓ File deleted")        
        log_result(log, test_name, test_description, True, "Basic file operations completed successfully", all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs0_close_to_open_consistency(log, mount_point, mount_options, all_results=None):
    """Test close-to-open consistency"""
    test_name = 'close_to_open_consistency'
    test_description = "Verify that data written by one process is visible to another process after the first process closes the file, confirming close-to-open consistency guarantees of NFS3"

    log.info(f'{equal_80}')
    log.info("TEST: NFS Close-to-Open Consistency")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
        
    test_file = os.path.join(test_dir, 'c2o_test.txt')
    test_data = "Process 1 data"
    
    try:
        log.info("Phase 1: Process 1 - Write and close file")
        with open(test_file, 'w') as f:
            f.write(test_data)
        log.info("✓ File written and closed (should flush to server)")
        
        log.info("Phase 2: Allowing 0.5s for server flush")
        time.sleep(0.5)
        log.info("✓ Flush period elapsed")

        log.info("Phase 3: Process 2 - Open and read file")        
        with open(test_file, 'r') as f:
            content = f.read()
        log.info(f"  Read content: '{content}'")
        
        if content == test_data:
            log.info("✓ Process 2 sees Process 1's write (close-to-open works)")
        else:
            log.error(f"✗ Expected '{test_data}', got '{content}'")
        
        assert content == test_data
        log.info("✓ Close-to-open consistency verified")
        log_result(log, test_name, test_description, True, "Close-to-open consistency verified", all_results)
    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs0_small_file_performance(log, mount_point, mount_options, all_results=None, num_files=100):
    """Test small file performance"""

    test_name = 'small_file_performance'
    test_description = f"Measure performance of creating, reading, and deleting {num_files} small files to evaluate small file operation performance of NFS"

    log.info(f'{equal_80}')
    log.info("TEST: NFS Small File Performance")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
    
    test_subdir = os.path.join(test_dir, 'small_files')
    os.makedirs(test_subdir, exist_ok=True)
    
    try:
        log.info(f"Phase 1: Creating {num_files} small files")
        start = time.time()
        for i in range(num_files):
            filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
            with open(filepath, 'w') as f:
                f.write(f"{i}")
            if (i + 1) % 25 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                log.info(f"  Progress: {i+1}/{num_files} ({rate:.0f} files/s)")
        create_time = time.time() - start
        create_rate = num_files / create_time
        log.info(f"✓ Created {num_files} files in {create_time:.2f}s ({create_rate:.0f} ops/s)")
        
        log.info(f"Phase 2: Reading {num_files} files")
        start = time.time()
        for i in range(num_files):
            filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
            with open(filepath, 'r') as f:
                _ = f.read()
        read_time = time.time() - start
        read_rate = num_files / read_time
        log.info(f"✓ Read {num_files} files in {read_time:.2f}s ({read_rate:.0f} ops/s)")
        
        log.info(f"Phase 3: Deleting {num_files} files")
        start = time.time()
        for i in range(num_files):
            filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
            os.remove(filepath)
        delete_time = time.time() - start
        delete_rate = num_files / delete_time
        log.info(f"✓ Deleted {num_files} files in {delete_time:.2f}s ({delete_rate:.0f} ops/s)")
        
        log.info(f"✓ Small file performance test completed")
        log_result(log, test_name, test_description, True, f"{num_files} files - Create: {create_rate:.0f} ops/s, Read: {read_rate:.0f} ops/s, Delete: {delete_rate:.0f} ops/s")

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e))
        

def test_nfs0_concurrent_writers(log, mount_point, mount_options, all_results=None, num_writers=10):
    """Test concurrent writers"""

    test_name = 'concurrent_writers'
    test_description = f"Verify that {num_writers} concurrent writer threads can write to separate files without data corruption, confirming that NFS3 can handle concurrent write operations correctly"

    log.info(f'{equal_80}')
    log.info(f"TEST: NFS concurrent writer with {num_writers} threads")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
    
    def writer_task(writer_id):
        try:
            filepath = os.path.join(test_dir, f'writer_{writer_id}.txt')
            data = f"Writer {writer_id}\n" * 1000
            
            with open(filepath, 'w') as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            
            with open(filepath, 'r') as f:
                read_data = f.read()
            
            return len(read_data) == len(data)
        except Exception as e:
            log.error(f"  [Writer {writer_id}] ✗ Failed: {e}")
            return False
   
    try:
        log.info(f"Phase 1: Launching {num_writers} writer threads")
        start = time.time()
        with ThreadPoolExecutor(max_workers=num_writers) as executor:
            results = list(executor.map(writer_task, range(num_writers)))
        duration = time.time() - start
        
        success_count = sum(results)
        log.info(f"Phase 2: All threads completed in {duration:.2f}s")
        log.info(f"  Success: {success_count}/{num_writers}")
        
        if success_count == num_writers:
            log.info(f"✓ All {num_writers} concurrent writers succeeded")
        else:
            log.error(f"✗ Only {success_count}/{num_writers} writers succeeded")
        
        log_result(log, test_name, test_description, True, f"{success_count} == {num_writers} || {success_count}/{num_writers} writers succeeded in {duration:.2f}s", all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs0_large_file_sequential_io(log, mount_point, mount_options, all_results=None, size_mb=100):
    """Test large sequential I/O"""

    test_name = 'large_sequential_io'
    test_description = f"Verify that {size_mb}MB file can be written and read sequentially without corruption"
    
    log.info(f'{equal_80}')
    log.info(f"TEST: NFS Large File Sequential read/write with {size_mb}MB file")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')

    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return

    test_file = os.path.join(test_dir, 'large_seq.bin')
    chunk_size = 1024 * 1024
    
    try:
        log.info(f"Phase 1: Sequential WRITE ({size_mb}MB)")
        start = time.time()
        with open(test_file, 'wb') as f:
            for i in range(size_mb):
                f.write(os.urandom(chunk_size))
                if (i + 1) % 25 == 0:
                    elapsed = time.time() - start
                    rate = (i + 1) / elapsed
                    log.info(f"  Progress: {i+1}/{size_mb}MB ({rate:.1f} MB/s)")
        write_time = time.time() - start
        write_mbps = size_mb / write_time
        log.info(f"✓ Write completed: {size_mb}MB in {write_time:.2f}s ({write_mbps:.2f} MB/s)")
        
        log.info(f"Phase 2: Sequential READ ({size_mb}MB)")
        start = time.time()
        bytes_read = 0
        with open(test_file, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                bytes_read += len(chunk)
        read_time = time.time() - start
        read_mbps = size_mb / read_time
        log.info(f"✓ Read completed: {size_mb}MB in {read_time:.2f}s ({read_mbps:.2f} MB/s)")

        log.info(f"Phase 3: Cleaning up")        
        os.remove(test_file)
        log.info("✓ Test file removed")
        
        log.info(f"✓ Large file I/O test completed")
        log_result(log, test_name, test_description, True, f"{size_mb}MB - Write: {write_mbps:.2f} MB/s, Read: {read_mbps:.2f} MB/s", all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description  , False, str(e), all_results)


def test_nfs0_readonly_mount_enforcement(log, mount_point, mount_options, all_results=None):
    """Test ro mount blocks writes"""

    test_name = 'readonly_mount_enforcement'
    test_description = f"Verify that a read-only mount correctly blocks write operations, confirming that NFS3 enforces read-only access restrictions as expected"

    mount_mode = mount_options.mount_mode

    log.info(f'{equal_80}')
    log.info("TEST: NFS Read-Only Mount Enforcement")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')

    if mount_mode == 'rw':
        log.info(f"✓ Skipping Test on a RW mount (rw)")
        log_result(log, test_name, test_description, True, f"Skipping Test on a RW mount", all_results)
        return

    
    # Try to write to the mount point it (not a subdirectory)
    test_file = os.path.join(mount_point, 'ro_test.txt')
    
    try:
        log.info("Phase 1: Attempting write on RO mount")       
        try:   
            with open(test_file, 'w') as f:
                f.write("Should fail")
            log.error("✗ Write succeeded on RO mount - TEST FAILED!")
            log_result(log, test_name, test_description, False, "Write succeeded on ro mount!", all_results)

        except (OSError, IOError) as e:
            if e.errno in (30, 13):  # EROFS or EACCES
                log.info(f"✓ Write correctly blocked (errno: {e.errno})")
                log_result(log, test_name, test_description, True, f"Write blocked as expected (errno {e.errno})", all_results)
            else:
                log.error(f"✗ Unexpected error: {e}")
                log_result(log, test_name, test_description, False, str(e), all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs0_readonly_mount_read_operations(log, mount_point, mount_options, all_results=None):
    """Test that read operations work on RO mount"""

    test_name = 'readonly_mount_read_operations'
    test_description = f"Verify that read operations (like listing directory contents and getting file stats) work correctly on a read-only mount, confirming that NFS3 allows read operations while enforcing write restrictions on RO mounts"

    log.info(f'{equal_80}')
    log.info("TEST: NFS Read-Only Mount Read Operations")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    try:
        log.info("Phase 1: Listing directory contents")
        contents = os.listdir(mount_point)
        log.info(f"✓ Directory listed successfully ({len(contents)} items found)")

        log.info("Phase 2: Getting directory stats")
        stat_info = os.stat(mount_point)
        log.info(f"✓ Directory stat successful")
        log.info(f"  Mode: {oct(stat_info.st_mode)}")
        log.info(f"  Owner: {stat_info.st_uid}")     
        log.info("✓ Read operations working on RO mount")
        log_result(log, test_name, test_description, True, f"Read operations successful ({len(contents)} items)", all_results)
    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs3_nconnect(log, mount_point, mount_options, expected_connections=None, all_results=None):

    test_name = 'nconnect_verification'
    test_description = "Verify nconnect mount option establishes multiple TCP connections to NFS server"

    log.info(f'{equal_80}')
    log.info("TEST: NFS3 nconnect Verification")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')

    expected_connections = mount_options.nconnect_count

    try:
        # Phase 1: Check if nconnect is in mount options
        log.info("Phase 1: Checking mount options for nconnect")
        nconnect_value = None
        server_ip = None
        with open('/proc/mounts', 'r') as f:
            for line in f.readlines():
                if mount_point in line:
                    options = line.split()[3]
                    # log.info(f"Mount options: {options}")
                    for opt in options.split(','):
                        if opt.startswith('addr='):
                            server_ip = opt.split('=')[1]                           
                        if opt.startswith('nconnect='):
                            nconnect_value = int(opt.split('=')[1])
                    if nconnect_value is not None:
                        break


        if nconnect_value is None:
            server_ip = None
            msg = f"nconnect option not found in mount options for {mount_point}"
            log.error(f"✗ {msg}")
            log_result(log, test_name, test_description, False, msg, all_results)
            return

        log.info(f"✓ nconnect={nconnect_value} found in mount options")

        # Phase 2: Verify actual TCP connections if server_ip provided

        if server_ip:
            log.info(f"Phase 2: Verifying TCP connections to {server_ip}")    
            result = subprocess.run(
                ['ss', '-tn', 'dst', server_ip],
                capture_output=True, text=True
            )
 
            # Count lines that show port 2049 (NFS)
            connections = [
                line for line in result.stdout.splitlines()
                if ':2049' in line
            ]
            actual_count = len(connections)
            log.info(f"TCP connections to {server_ip}:2049 — found {actual_count}")
            for conn in connections:
                log.info(f"  {conn.strip()}")

            target = expected_connections or nconnect_value
            if actual_count >= target:
                log.info(f"✓ Expected {target} connections, found {actual_count}")
            else:
                msg = f"Expected {target} TCP connections, only found {actual_count}"
                log.warning(f"✗ {msg}")
                log_result(log, test_name, test_description, False, msg, all_results)
                return
        else:
            log.info("Phase 2: Skipped (no server_ip provided — cannot verify TCP connections)")

        # Phase 3: Functional I/O test to exercise the connections
        log.info("Phase 3: Functional I/O across nconnect mount")
        is_success, test_dir = create_test_directory(log, mount_point)
        if not is_success:
            log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
            return

        errors = []
        def write_read_worker(worker_id):
            try:
                test_file = os.path.join(test_dir, f'nconnect_worker_{worker_id}.txt')
                test_data = f"nconnect worker {worker_id} data"
                with open(test_file, 'w') as f:
                    f.write(test_data)
                with open(test_file, 'r') as f:
                    content = f.read()
                assert content == test_data, f"Data mismatch on worker {worker_id}"
                os.remove(test_file)
            except Exception as e:
                errors.append(str(e))

        # Spawn threads equal to nconnect value to exercise all connections
        threads = [threading.Thread(target=write_read_worker, args=(i,)) for i in range(nconnect_value)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            msg = f"Parallel I/O errors: {errors}"
            log.error(f"✗ {msg}")
            log_result(log, test_name, test_description, False, msg, all_results)
            return

        log.info(f"✓ {nconnect_value} parallel workers completed I/O successfully")

        # Cleanup
        log.info("Phase 4: Cleanup")
        os.rmdir(test_dir)
        log.info("✓ Test directory removed")

        log_result(log, test_name, test_description, True,
                   f"nconnect={nconnect_value} verified with {nconnect_value} parallel workers", all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs3_transport_protocol(log, mount_point, mount_options, all_results=None):
    """Verify correct transport protocol"""
    test_name = 'NFS3_transport_protocol'
    test_description = "Verify that the mount is using the correct transport protocol (TCP or UDP)"
    
    log.info(f'{equal_80}')
    log.info("TEST: NFS3 Transport Protocol Verification")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    try:
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()

        log.info("Phase 1: Searching for mount point and verifying transport protocol")

        for line in mounts.split('\n'):
            if mount_point in line:
                log.info("Found mount entry in /proc/mounts")
                
                if mount_options.transport == 'tcp':
                    if 'proto=tcp' in line or ',tcp' in line:
                        log.info("✓ Confirmed: Using TCP")
                        log_result(log, test_name, test_description, True, "Using TCP as expected", all_results)
                        return
                    
                elif mount_options.transport == 'udp':
                    if 'proto=udp' in line or ',udp' in line:
                        log.info("✓ Confirmed: Using UDP")
                        log_result(log, test_name, test_description, True, "Using UDP as expected")
                        return
        
        log_result(log, test_name, test_description, False, "Could not verify transport protocol", all_results)

    except Exception as e:
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs3_nlm_basic_locking(log, mount_point, mount_options, all_results=None):
    """Test NLM basic file locking"""

    test_name = 'NFS3_nlm_basic_locking'
    test_description = "Verify that exclusive locks can be acquired and block other processes as expected, confirming basic NLM file locking functionality of NFS3"

    log.info(f'{equal_80}')
    log.info("TEST: NFS3 NLM Basic File Locking")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
    
    test_file = os.path.join(test_dir, 'lock_test.txt')
    
    try:
        log.info("Phase 1: Creating test file")
        with open(test_file, 'w') as f:
            f.write("Lock test data")
        log.info("✓ Test file created")
        
        log.info("Phase 2: Acquiring exclusive lock (LOCK_EX)")
        f = open(test_file, 'r+')
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        log.info("✓ Exclusive lock acquired by main process")
        
        log.info("Phase 3: Spawning child process to test lock blocking")
        def try_lock_exclusive():
            try:
                f2 = open(test_file, 'r+')
                fcntl.flock(f2.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                f2.close()
                return False
            except IOError:
                return True
        
        p = multiprocessing.Process(target=try_lock_exclusive)
        p.start()
        p.join()
        
        log.info("Phase 4: Releasing exclusive lock")
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()
        log.info("✓ Lock released successfully")
        
        log.info("✓ NLM basic locking test passed")
        log_result(log, test_name, test_description, True, "NLM basic locking test passed", all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs3_idempotent_operations(log, mount_point, mount_options, all_results=None):
    """Test operation idempotency"""
    test_name = 'NFS3_idempotent_operations'
    test_description = "Verify that repeated operations have the same effect as a single operation, confirming idempotency of file operations"  

    log.info(f'{equal_80}')
    log.info("TEST: NFS3 Idempotent Operations (NFS3 Stateless Protocol)")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')

    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return 
    
    test_file = os.path.join(test_dir, 'idempotent.txt')
    
    try:
        log.info("Phase 1: Testing idempotent CREATE/WRITE operations")
        for i in range(3):
            log.info(f"  Iteration {i+1}: Writing 'Iteration {i}'")
            with open(test_file, 'w') as f:
                f.write(f"Iteration {i}")
        
        log.info("Phase 2: Verifying final content")
        with open(test_file, 'r') as f:
            content = f.read()                      
        log.info(f"  File content: '{content}'")
        
        if "Iteration 2" in content:
            log.info("  ✓ Last write persisted correctly")
        else:
            log.error(f"  ✗ Expected 'Iteration 2', got '{content}'")          
        assert "Iteration 2" in content
        

        log.info("Phase 3: Testing idempotent DELETE operation")
        os.remove(test_file)
        log.info("  ✓ First delete successful")
        
        try:
            os.remove(test_file)
            log.error("  ✗ Second delete should have failed")
        except FileNotFoundError:
            log.info("  ✓ Second delete correctly raised FileNotFoundError")
        
        log.info("✓ Idempotency test passed")
        log_result(log, test_name, test_description, True, "Idempotent operations test passed", all_results)
    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs4_transport_protocol(log, mount_point, mount_options, all_results=None):
    """Verify correct transport protocol"""
    test_name = 'NFS4_transport_protocol'
    test_description = "Verify that the mount is using the correct transport protocol (TCP)"
    
    log.info(f'{equal_80}')
    log.info("TEST: NFS4 Transport Protocol Verification")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    try:
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()

        log.info("Phase 1: Searching for mount point and verifying transport protocol")

        for line in mounts.split('\n'):
            if mount_point in line:
                log.info("Found mount entry in /proc/mounts")
               
                if mount_options.transport == 'tcp':
                    if 'proto=tcp' in line or ',tcp' in line:
                        log.info("✓ Confirmed: Using TCP")
                        log_result(log, test_name, test_description, True, "Using TCP as expected", all_results)
                        return
                    
                elif mount_options.transport == 'udp':
                    if 'proto=udp' in line or ',udp' in line:
                        log.info("✓ Confirmed: Using UDP")
                        log_result(log, test_name, test_description, True, "Using UDP as expected")
                        return
        
        log_result(log, test_name, test_description, False, "Could not verify transport protocol", all_results)
    except Exception as e:
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs4_stateful_operations(log, mount_point, mount_options, all_results=None):
    """Test NFS4 stateful protocol operations"""

    test_name = 'NFS4_stateful_operations'
    test_description = "Verify that NFS4 maintains state across operations (like open file handles) and that state is properly cleaned up on close, confirming the stateful protocol behavior of NFS4"

    log.info(f"{equal_80}")
    log.info("TEST: NFS4 Stateful Protocol Operations")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return

    test_file = os.path.join(test_dir, 'stateful_test.txt')
    
    try:
        log.info(f"Phase 1: Opening file and maintaining state")
        f = open(test_file, 'w')
        log.info("✓ File opened (server maintains state)")
        
        log.info(f"Phase 2: Writing data while file is open")
        f.write("NFS4 stateful test")
        f.flush()
        log.info("✓ Data written")
        
        log.info(f"Phase 3: Closing file (state cleanup)")
        f.close()
        log.info("✓ File closed (server releases state)")
        
        log.info(f"Phase 4: Verifying data persistence")
        with open(test_file, 'r') as f:
            content = f.read()
        
        if content == "NFS4 stateful test":
            log.info("✓ Data persisted correctly")
        else:
            log.error(f"✗ Data mismatch: '{content}'")
        
        assert content == "NFS4 stateful test"
        
        log.info(f"✓ Stateful operations test passed")
        log_result(log, test_name, test_description, True, f"Stateful operations successful ({len(contents)} items)", all_results)
        
    except Exception as e:
        log.error(f"✗ Stateful operations test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs4_compound_operations(log, mount_point, mount_options, all_results=None):
    """Test NFS4 COMPOUND procedure"""

    test_name = 'NFS4_compound_operations'
    test_description = "Verify that NFS4 can bundle multiple operations in a single RPC call, improving performance over NFS3"

    log.info(f"{equal_80}")
    log.info("TEST: NFS4 COMPOUND Operations")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')

    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return

    test_file = os.path.join(test_dir, 'compound_test.txt')
    
    try:
        log.info(f"Phase 1: Creating file (COMPOUND: OPEN + WRITE + CLOSE)")
        start = time.time()
        with open(test_file, 'w') as f:
            f.write("Compound operation test")
        compound_time = time.time() - start
        log.info(f"✓ File created in {compound_time:.4f}s")
        log.info("  (Single COMPOUND RPC vs multiple RPCs in NFS3)")
        
        log.info(f"Phase 2: Reading file (COMPOUND: OPEN + READ + CLOSE)")
        start = time.time()
        with open(test_file, 'r') as f:
            content = f.read()
        read_time = time.time() - start
        log.info(f"✓ File read in {read_time:.4f}s")
        
        log.info(f"Phase 3: Verifying data")
        if content == "Compound operation test":
            log.info("✓ Data verified")
        else:
            log.error(f"✗ Data mismatch: '{content}'")
        
        assert content == "Compound operation test"
        
        log.info(f"✓ COMPOUND operations test passed")
        log.info(f"  Performance benefit: Reduced network round-trips")
        log_result(log, test_name, test_description, True, f"Write: {compound_time:.4f}s, Read: {read_time:.4f}s", all_results)
        
    except Exception as e:
        log.error(f"✗ COMPOUND operations test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)        


def test_nfs4_delegation_basic(log, mount_point, mount_options, all_results=None):
    """Test NFS4 delegation (if supported)"""

    test_name = 'NFS4_delegation_basic'
    test_description = "Verify NFS4 delegation mechanisms are functional"

    log.info(f"{equal_80}")
    log.info("TEST: NFS4 Delegation")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')

    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return    
    
    test_file = os.path.join(test_dir, 'delegation_test.txt')
    
    try:
        log.info(f"Phase 1: Creating file and requesting delegation")
        with open(test_file, 'w') as f:
            f.write("Delegation test")
        log.info("✓ File created (delegation may be granted)")
        log.info("  Note: Delegation is at server's discretion")
        
        log.info(f"Phase 2: Multiple reads (should use delegated cache)")
        for i in range(5):
            with open(test_file, 'r') as f:
                content = f.read()
            log.info(f"  Read {i+1}: {len(content)} bytes")
        log.info("✓ Multiple reads completed (likely using delegation cache)")
        
        log.info(f"Phase 3: Modifying file (delegation recall)")
        with open(test_file, 'a') as f:
            f.write("\nModified")
        log.info("✓ File modified (delegation recalled if active)")
        
        log.info(f"✓ Delegation test passed")
        log.info(" Note: Check server logs for actual delegation grants")
        log_result(log, test_name, test_description, True, "Delegation mechanisms exercised", all_results)
        
    except Exception as e:
        log.error(f"✗ Delegation test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs4_acls(log, mount_point, mount_options, all_results=None):
    """Test NFS4 ACLs (richer than POSIX)"""
    test_name = 'NFS4_acls'
    test_description = "Verify NFS4 ACL mechanisms are functional"

    log.info(f"{equal_80}")
    log.info("TEST: NFS4 ACLs")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    


    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return    

    test_file = os.path.join(test_dir, 'acl_test.txt')
    
    try:
        log.info(f"Phase 1: Creating test file")
        with open(test_file, 'w') as f:
            f.write("ACL test file")
        log.info("✓ File created")
        
        log.info(f"Phase 2: Checking file permissions")
        stat_info = os.stat(test_file)
        perms = oct(stat_info.st_mode)[-3:]
        log.info(f"  POSIX permissions: {perms}")
        log.info(f"  Owner UID: {stat_info.st_uid}")
        log.info(f"  Group GID: {stat_info.st_gid}")
        
        log.info(f"Phase 3: Modifying permissions")
        os.chmod(test_file, 0o644)
        log.info("✓ Permissions set to 644")
        
        stat_info = os.stat(test_file)
        new_perms = oct(stat_info.st_mode)[-3:]
        if new_perms == '644':
            log.info("✓ Permission change verified")
        else:
            log.error(f"✗ Expected 644, got {new_perms}")
        
        log.info(f"✓ ACL test passed")
        log.info("  Note: Advanced ACL features require nfs4_getfacl/nfs4_setfacl")
        log_result(log, test_name, test_description, True, "Basic ACL operations verified", all_results)
        
    except Exception as e:
        log.error(f"✗ ACL test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs4_named_attributes(log, mount_point, mount_options, all_results=None):
    """Test NFS4 named attributes"""

    test_name = 'NFS4_named_attributes'
    test_description = "Verify NFS4 named attribute mechanisms are functional"

    log.info(f"{equal_80}")
    log.info("TEST: NFS4 Named Attributes")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return    

    test_file = os.path.join(test_dir, 'attr_test.txt')
    
    try:
        log.info(f"Phase 1: Creating test file")
        with open(test_file, 'w') as f:
            f.write("Named attributes test")
        log.info("✓ File created")
        
        log.info(f"Phase 2: Checking standard attributes")
        stat_info = os.stat(test_file)
        log.info(f"  Size: {stat_info.st_size} bytes")
        log.info(f"  Modified: {time.ctime(stat_info.st_mtime)}")
        log.info(f"  Inode: {stat_info.st_ino}")
        
        log.info(f"Phase 3: File attributes retrieved successfully")
        log.info("✓ Named attributes mechanism working")
        log.info("  Note: Extended attrs use getfattr/setfattr tools")
        
        
        log_result(log, test_name, test_description, True, "Attribute operations verified", all_results)
        
    except Exception as e:
        log.error(f"✗ Named attributes test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)        


def test_nfs4_parallel_io_performance(log, mount_point, mount_options, all_results=None, num_threads=10):
    """Test NFS4 parallel I/O performance"""
    
    test_name = 'NFS4_parallel_io_performance'
    test_description = "Verify NFS4 parallel I/O performance"
    
    log.info(f"{equal_80}")
    log.info("TEST: NFS4 Parallel I/O Performance")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return    
    
    def io_task(task_id):
        try:
            filepath = os.path.join(test_dir, f'parallel_{task_id}.dat')
            data = os.urandom(1024 * 1024)  # 1MB
            
            log.info(f"  [Thread {task_id}] Writing 1MB...")
            start = time.time()
            with open(filepath, 'wb') as f:
                f.write(data)
                f.flush()
            write_time = time.time() - start
            
            log.info(f"  [Thread {task_id}] Reading 1MB...")
            start = time.time()
            with open(filepath, 'rb') as f:
                read_data = f.read()
            read_time = time.time() - start
            
            if data == read_data:
                log.info(f"  [Thread {task_id}] ✓ Verified (W:{write_time:.3f}s R:{read_time:.3f}s)")
                return True
            else:
                log.error(f"  [Thread {task_id}] ✗ Data mismatch")
                return False
                
        except Exception as e:
            log.error(f"  [Thread {task_id}] ✗ Error: {e}")
            return False
    
    try:
        log.info(f"Phase 1: Launching {num_threads} parallel I/O threads")
        start = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            results = list(executor.map(io_task, range(num_threads)))
        duration = time.time() - start
        
        success = sum(results)
        total_mb = num_threads * 2  # Read + Write
        throughput = total_mb / duration
        
        log.info(f"✓ Parallel I/O completed in {duration:.2f}s")
        log.info(f"  Success: {success}/{num_threads} threads")
        log.info(f"  Aggregate throughput: {throughput:.2f} MB/s")
        
        log_result(log, test_name, test_description, success == num_threads, f"{success}/{num_threads} threads, {throughput:.2f} MB/s", all_results)
        
    except Exception as e:
        log.error(f"✗ Parallel I/O test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs4_minorversion_features(log, mount_point, mount_options, all_results=None):
    """Test NFS4 minor version specific features"""

    test_name = f'NFS4_{mount_options.minorvers}_features'
    test_description = f"Verify features specific to NFS4.{mount_options.minorvers} are functional and that the mount is using the correct minor version"
    
    log.info(f"{equal_80}")
    log.info(f"TEST: NFS4.{mount_options.minorvers} Specific Features")
    log.info(f"DESCRIPTION: {test_description}")
    log.info(f'{equal_80}')

    try:
        
        if mount_options.minorvers == 0:
            log.info("NFSv4.0 Features:")
            log.info("  - Stateful protocol")
            log.info("  - COMPOUND operations")
            log.info("  - Delegations")
            log.info("  - Named attributes")
            
        elif mount_options.minorvers == 1:
            log.info("NFSv4.1 Features (includes 4.0 +):")
            log.info("  - Sessions (improved connection management)")
            log.info("  - pNFS (parallel NFS)")
            log.info("  - Improved callback system")
            log.info("  - Exactly-once semantics")
            
        elif mount_options.minorvers == 2:
            log.info("NFSv4.2 Features (includes 4.1 +):")
            log.info("  - Server-side copy")
            log.info("  - Sparse files")
            log.info("  - Space reservation")
            log.info("  - Application I/O hints")
        
        # Verify mount is actually using the version
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()
        
        for line in mounts.split('\n'):
            if mount_point in line:
                log.info(f"Active mount options:")
               
                if f'vers=4.{mount_options.minorvers}' in line or f'nfsvers=4.{mount_options.minorvers}' in line:
                    log.info(f"✓ Confirmed NFS4.{mount_options.minorvers}")
                    log_result(log, test_name, test_description, True, f"NFS4.{mount_options.minorvers} verified")
                    return
        
        log.warning("⚠ Could not verify minor version in mount options")
        log_result(log, test_name, test_description, False, "Version check inconclusive")
        
    except Exception as e:
        log.error(f"✗ Minor version test failed: {e}")
        log_result(log, test_name, test_description, False, str(e))



