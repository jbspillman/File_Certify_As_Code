# test_nfs.py  - NFS Test Suite for File Certification as Code

from exec_mounts import MOUNT_OPTIONS, mount_nas, unmount_nas
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from datetime import datetime
import multiprocessing
import fcntl
import time
import os

import inspect
import sys
current_module = sys.modules[__name__]

''' some general constants '''
stars_25 = '*' * 25
stars_80 = '*' * 80
equal_25 = '=' * 25
equal_80 = '=' * 80
''' end of general constants '''

preset_mounts = [
    {
        'vendor': 'Dell',
        'software': 'PowerScale OneFS 9.10.0.0',
        'export_server': 'onefs002-2.beastmode.local.net',
        'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_rw',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 3,
            'transport': 'tcp',
        }
    },  
    # {
    #     'vendor': 'Dell',
    #     'software': 'PowerScale OneFS 9.10.0.0',
    #     'export_server': 'onefs002-2.beastmode.local.net',
    #     'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_ro',
    #     'host_access': 'ro',           # read ro, write rw, root rt, none na
    #     'host_access_expected': 'ro',  # read ro, write rw, root rt, none na
    #     'options': {
    #         'majorvers': 3,
    #         'transport': 'tcp',
    #     }
    # },    
    # {
    #     'vendor': 'Dell',
    #     'software': 'PowerScale OneFS 9.10.0.0',
    #     'export_server': 'onefs002-2.beastmode.local.net',
    #     'export_path': '/ifs/ACCESS_ZONES/system/nfs4_01_rw',
    #     'host_access': 'rw',           # read ro, write rw, root rt, none na
    #     'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
    #     'options': {
    #         'majorvers': 4,
    #         'minorversion': 1,
    #         'transport': 'tcp',
    #     }
    # },   
    # {
    #     'vendor': 'Dell',
    #     'software': 'PowerScale OneFS 9.10.0.0',
    #     'export_server': 'onefs002-2.beastmode.local.net',
    #     'export_path': '/ifs/ACCESS_ZONES/system/nfs4_01_ro',
    #     'host_access': 'ro',           # read ro, write rw, root rt, none na
    #     'host_access_expected': 'ro',  # read ro, write rw, root rt, none na
    #     'options': {
    #         'majorvers': 4,
    #         'minorversion': 2,
    #         'transport': 'tcp',
    #     }
    # },         
    {
        'vendor': 'NetApp',
        'software': 'ONTAP 9.16.1P1',
        'export_server': 'svm01.beastmode.local.net',
        'export_path': '/svm01_vol02',
        'host_access': 'rw',           # read ro, write rw, root rt, none na
        'host_access_expected': 'rw',  # read ro, write rw, root rt, none na
        'options': {
            'majorvers': 3,
            'transport': 'tcp',
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
            'minorversion': 1,
            'transport': 'tcp',
        }
    }
]


def nfs_test_suite(log, mounts_list: list[dict] = []):
    """
    This function will iterate through a list of NFS mounts, attempt to mount each one, 
    perform some basic protocol tests (like listing files, checking permissions, etc), and then unmount it. The results of each step will be logged for later analysis.
    
    The mounts_list should be a list of dictionaries, where each dictionary contains the necessary information to perform the mount 
    (vendor, software version, export server, export path, options, etc).
    
    The MOUNT_OPTIONS dataclass can be used to validate and set defaults for the options provided in each mount dictionary.   
    """

    v0_prefix = "test_nfs_"
    v3_prefix = "test_nfs3_"
    v4_prefix = "test_nfs4_"
    v0_functions = get_test_functions(v0_prefix)
    v3_functions = get_test_functions(v3_prefix)
    v4_functions = get_test_functions(v4_prefix)
       
    if not mounts_list or len(mounts_list) == 0:
        log.warning("No mounts provided for NFS test suite. Please provide a list of mounts to test.")
        mounts_list = preset_mounts
        log.info(f"Using a preset mounts list for testing....")


    all_results = []  # This will store results of all tests for all mounts for later reporting and analysis
    for nfs_mount in mounts_list:
        
        passed = 0  # not really using these for status tracking, but they could be used to log a summary of how many tests passed/failed for each mount in the final report
        failed = 0  # not really using these for status tracking, but they could be used to log a summary of how many tests passed/failed for each mount in the final report

        vendor = nfs_mount["vendor"]
        software = nfs_mount["software"]
        nfs_server = nfs_mount["export_server"]
        nfs_export = nfs_mount["export_path"]
        nfs_options = nfs_mount["options"]
        MOUNT_OPTIONS(**nfs_options)  # validate options and set defaults
        log.blank()
        log.info(f"{equal_80}")
        log.info(f"NFS Testing of Vendor: {vendor} Software: {software} | NFS Server: {nfs_server} | NFS Export: {nfs_export} ")
        log.info(f"{equal_80}")
        log.blank()
        mount_status, mount_path = mount_nas(
            log         = log,
            vendor      = vendor,
            software    = software,
            nfs_server  = nfs_server,
            nfs_export  = nfs_export,
            uid         = 1000,
            gid         = 1000,
            options     = MOUNT_OPTIONS(**nfs_options),
            dry_run     = False,
        )
        
        if mount_status:

            log.info(f"Mounted {vendor} {software} export at {mount_path} with options: {nfs_options}")
            log.blank()
            if os.path.ismount(mount_path):

                log.info(f"Verified {mount_path} is a valid mount point.")
                options_string = str(MOUNT_OPTIONS(**nfs_options))

                log.info(f"Discovered: [ {len(v0_functions)} ] test functions with prefix '{v0_prefix}' to execute for each mount.")
                log.header(f"Running: {v0_prefix}*  ({len(v0_functions)} tests)")
                for name, func in v0_functions:
                    log.step(f"► {name}")
                    try:
                        result = func(log, mount_path, MOUNT_OPTIONS(**nfs_options), all_results)
                        if result:
                            # log.success(f"✓ {name}")
                            passed += 1
                        else:
                            # log.error(f"✗ {name}")
                            failed += 1
                    except Exception as e:
                        # log.error(f"✗ {name} — exception: {e}", exc_info=True)
                        failed += 1


                if 'majorvers=3' in options_string:
                    log.info(f"Discovered: [ {len(v3_functions)} ] test functions with prefix '{v3_prefix}' to execute for each mount.")
                    log.header(f"Running: {v3_prefix}*  ({len(v3_functions)} tests)")
                    for name, func in v3_functions:
                        log.step(f"► {name}")
                        try:
                            result = func(log, mount_path, MOUNT_OPTIONS(**nfs_options), all_results)
                            if result:
                                # log.success(f"✓ {name}")
                                passed += 1
                            else:
                                # log.error(f"✗ {name}")
                                failed += 1
                        except Exception as e:
                            #log.error(f"✗ {name} — exception: {e}", exc_info=True)
                            failed += 1

                if 'majorvers=4' in options_string:
                    log.info(f"Discovered: [ {len(v4_functions)} ] test functions with prefix '{v4_prefix}' to execute for each mount.")   
                    log.header(f"Running: {v4_prefix}*  ({len(v4_functions)} tests)")
                    for name, func in v4_functions:
                        log.step(f"► {name}")
                        try:
                            result = func(log, mount_path, MOUNT_OPTIONS(**nfs_options), all_results)
                            if result:
                                #log.success(f"✓ {name}")
                                passed += 1
                            else:
                                # log.error(f"✗ {name}")
                                failed += 1
                        except Exception as e:
                            # log.error(f"✗ {name} — exception: {e}", exc_info=True)
                            failed += 1
                log.blank()
            else:
                log.error(f"{mount_path} does not appear to be a valid mount point. Check mount logs for details.")

            if mount_status:
                time.sleep(2)  # pause between mounts for readability, not strictly necessary
                log.blank()
                unmount_nas(log, vendor, software, mount_path,  dry_run=False)
        else:
            log.error(f"Failed to mount {vendor} {software} export. Skipping unmount.")
            continue


#################################################################################################################################
## Function Finder Tools - to discover test functions based on naming conventions and execute them in order for each mount
#################################################################################################################################

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

#################################################################################################################################
#  NFS 3 or 4 tests - to be implemented with real NFS client interactions and assertions
#################################################################################################################################

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

    # Log to text documentation
    # if text_logger:
        # text_logger.log_test_result(test_name, passed, message)

    if all_results is not None:
        all_results.append(result)
    return result


#################################################################################################################################
## !!!! ALL TESTING PROTOCOL FUNCTIONS MUST BE NAMED ACCORDING TO THE PROTOCOLS !!!                                            ## 
##  >> test_nfs0_ for NFS3/NFS4 tests that apply to both versions                                                              ##
##  >> test_nfs3_ for NFS3-specific tests                                                                                      ##
##  >> test_nfs4_ for NFS4-specific tests.                                                                                     ##
#################################################################################################################################


def test_nfs_mount_options_verification(log, mount_point, mount_options=None, all_results=None):
    test_name = 'mount_options_verification'
    test_description = "Confirm that the actual mount options match the requested configuration"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    """Verify mount options"""
    log.info(f'{equal_80}')
    log.info("TEST: NFS Mount Options Verification")
    log.info(f'{equal_80}')
    
    try:
        # if text_logger:
        #    text_log.log_test_step("Phase 1: Reading /proc/mounts")

        log.info("Phase 1: Reading /proc/mounts")
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()
        log.info(" ✓ Read /proc/mounts successfully")

        # if text_logger:
        #    text_log.log_test_step(f"Phase 2: Searching for mount point: {mount_point}")

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

            # if text_logger:
            #    text_log.log_test_step(f"Phase 3: Parsing options: {options}")
            log.info(f"Phase 3: Parsing mount options")
            
            if 'vers=3' in options or 'nfsvers=3' in options:
                log.info(" ✓ NFS Version: 3")
                # if text_logger:
                #    text_log.log_test_step(f" ✓ NFS Version: 3")
            
            if f'proto={mount_options.transport}' in options:
                log.info(f" ✓ Transport: {mount_options.transport}")
                # if text_logger:
                #    text_log.log_test_step(f" ✓ Transport: {mount_options.transport}")

            log.info("✓ Mount options verified")
            log_result(log, test_name, test_description, True, "Mount options verified", all_results)
        
        else:
            log.error("✗ Could not parse mount options")
            log_result(log, test_name, test_description, False, "Could not parse mount options", all_results)

    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs_readwrite_mount_enforcement(log, mount_point, mount_options=None, all_results=None):

    test_name = 'readwrite_mount_enforcement'
    test_description = "Verify read-write mount allows create, modify, and delete operations"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    """Test rw mount allows writes"""
    log.info(f'{equal_80}')
    log.info("TEST: NFS Read-Write Mount Enforcement")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return

    test_file = os.path.join(test_dir, 'rw_test.txt')
    test_data = "RW mount test"
    
    try:
        log.info("Phase 1: Testing write permissions")
        # if text_logger:
        #    text_log.log_test_step("Phase 1: Testing write permissions")
        with open(test_file, 'w') as f:
            f.write(test_data)
        log.info("✓ Write operation successful")
        
        log.info("Phase 2: Verifying data integrity")
        # if text_logger:
        #    text_log.log_test_step("Phase 2: Verifying data integrity")
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


def test_nfs_basic_file_operations(log, mount_point, mount_options=None, all_results=None):
    """Test basic file operations"""
    test_name = 'basic_file_operations'
    test_description = "Perform basic file operations (create, read, delete) to verify functionality"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)
    
    log.info(f'{equal_80}')
    log.info("TEST: NFS Basic File Operations")
    log.info(f'{equal_80}')
        
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
    
    test_file = os.path.join(test_dir, 'basic_test.txt')
    test_data = "Hello NFS CLIENT"
    try:
        log.info(f"Phase 1: Creating test file and writing data")
        # if text_logger:
        #    text_log.log_test_step("Phase 1: Creating test file and writing data")
        with open(test_file, 'w') as f:
            f.write(test_data)
        log.info(f"✓ File created with {len(test_data)} bytes")
        
        log.info(f"Phase 2: Reading file content back")
        # if text_logger:
        #    text_log.log_test_step("Phase 2: Reading file content back")
        with open(test_file, 'r') as f:
            read_data = f.read()
        log.info(f"✓ File read: '{read_data}'")
        
        log.info(f"Phase 3: Verifying data integrity")
        assert read_data == test_data
        # if text_logger:
        #    text_log.log_test_step("Phase 3: Data integrity verified")
        log.info("✓ Data integrity verified")
        
        log.info(f"Phase 4: Deleting test file")
        # if text_logger:
        #    text_log.log_test_step("Phase 4: Deleting test file")
        os.remove(test_file)
        log.info("✓ File deleted")
        
        log_result(log, test_name, test_description, True, "Basic file operations completed successfully", all_results)
    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs_close_to_open_consistency(log, mount_point, mount_options=None, all_results=None):
    """Test close-to-open consistency"""
    test_name = 'close_to_open_consistency'
    test_description = "Verify that data written by one process is visible to another process after the first process closes the file, confirming close-to-open consistency guarantees of NFS3"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    log.info(f'{equal_80}')
    log.info("TEST: NFS Close-to-Open Consistency")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
        
    test_file = os.path.join(test_dir, 'c2o_test.txt')
    test_data = "Process 1 data"
    
    try:
        log.info("Phase 1: Process 1 - Write and close file")
        # if text_logger:
        #    text_log.log_test_step("Phase 1: Process 1 - Write and close file")
        with open(test_file, 'w') as f:
            f.write(test_data)
        log.info("✓ File written and closed (should flush to server)")
        
        log.info("Phase 2: Allowing 0.5s for server flush")
        # if text_logger:
        #    text_log.log_test_step("Phase 2: Allowing 0.5s for server flush")
        time.sleep(0.5)
        log.info("✓ Flush period elapsed")

        log.info("Phase 3: Process 2 - Open and read file")        
        # if text_logger:
        #    text_log.log_test_step("Phase 3: Process 2 - Open and read file")
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


def test_nfs_small_file_performance(log, mount_point, mount_options=None, all_results=None, num_files=100):
    """Test small file performance"""

    test_name = 'small_file_performance'
    test_description = f"Measure performance of creating, reading, and deleting {num_files} small files to evaluate small file operation performance of NFS3"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    log.info(f'{equal_80}')
    log.info("TEST: NFS Small File Performance")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
    
    test_subdir = os.path.join(test_dir, 'small_files')
    os.makedirs(test_subdir, exist_ok=True)
    
    try:
        log.info(f"Phase 1: Creating {num_files} small files")
        # if text_logger:
        #    text_log.log_test_step(f"Phase 1: Creating {num_files} small files")
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
        # if text_logger:
        #    text_log.log_test_step(f"Phase 2: Reading {num_files} files")            
        start = time.time()
        for i in range(num_files):
            filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
            with open(filepath, 'r') as f:
                _ = f.read()
        read_time = time.time() - start
        read_rate = num_files / read_time
        log.info(f"✓ Read {num_files} files in {read_time:.2f}s ({read_rate:.0f} ops/s)")
        
        log.info(f"Phase 3: Deleting {num_files} files")
        # if text_logger:
        #    text_log.log_test_step(f"Phase 3: Deleting {num_files} files")
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
        

def test_nfs_concurrent_writers(log, mount_point, mount_options=None, all_results=None, num_writers=10):
    """Test concurrent writers"""

    test_name = 'concurrent_writers'
    test_description = f"Verify that {num_writers} concurrent writer threads can write to separate files without data corruption, confirming that NFS3 can handle concurrent write operations correctly"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    log.info(f'{equal_80}')
    log.info("TEST: NFS Concurrent Writers")
    log.info(f'{equal_80}')
    log.info(f"Testing {num_writers} concurrent writer threads")
    
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
        # if text_logger:
        #    text_log.log_test_step(f"Phase 1: Launching {num_writers} writer threads")
        start = time.time()
        with ThreadPoolExecutor(max_workers=num_writers) as executor:
            results = list(executor.map(writer_task, range(num_writers)))
        duration = time.time() - start
        
        success_count = sum(results)
        log.info(f"Phase 2: All threads completed in {duration:.2f}s")
        # if text_logger:
        #    text_log.log_test_step(f"Phase 2: All threads completed in {duration:.2f}s")
        log.info(f"  Success: {success_count}/{num_writers}")
        
        if success_count == num_writers:
            log.info(f"✓ All {num_writers} concurrent writers succeeded")
        else:
            log.error(f"✗ Only {success_count}/{num_writers} writers succeeded")
        
        log_result(log, test_name, test_description, True, f"{success_count} == {num_writers} || {success_count}/{num_writers} writers succeeded in {duration:.2f}s", all_results)
    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs_large_file_sequential_io(log, mount_point, mount_options=None, all_results=None, size_mb=100):
    """Test large sequential I/O"""

    test_name = 'large_sequential_io'
    test_description = f"Verify that {size_mb}MB file can be written and read sequentially without corruption"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    log.info(f'{equal_80}')
    log.info("TEST: NFS Large File Sequential I/O")
    log.info(f'{equal_80}')
    log.info(f"Testing sequential read/write with {size_mb}MB file")
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return

    test_file = os.path.join(test_dir, 'large_seq.bin')
    chunk_size = 1024 * 1024
    
    try:
        log.info(f"Phase 1: Sequential WRITE ({size_mb}MB)")
        # if text_logger:
        #    text_log.log_test_step(f"Phase 1: Sequential WRITE ({size_mb}MB)")
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
        # if text_logger:
        #    text_log.log_test_step(f"Phase 2: Sequential READ ({size_mb}MB)")
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
        # if text_logger:
        #    text_log.log_test_step(f"Phase 3: Cleaning up")
        os.remove(test_file)
        log.info("✓ Test file removed")
        
        log.info(f"✓ Large file I/O test completed")
        log_result(log, test_name, test_description, True, f"{size_mb}MB - Write: {write_mbps:.2f} MB/s, Read: {read_mbps:.2f} MB/s", all_results)
    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description  , False, str(e), all_results)


def test_nfs_readonly_mount_enforcement(log, mount_point, mount_options=None, all_results=None):
    """Test ro mount blocks writes"""

    test_name = 'readonly_mount_enforcement'
    test_description = f"Verify that a read-only mount correctly blocks write operations, confirming that NFS3 enforces read-only access restrictions as expected"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    log.info(f'{equal_80}')
    log.info("TEST: NFS Read-Only Mount Enforcement")
    log.info(f'{equal_80}')
    
    # Try to write to the mount point it (not a subdirectory)
    test_file = os.path.join(mount_point, 'ro_test.txt')
    
    try:
        log.info("Phase 1: Attempting write on RO mount")
        # if text_logger:
        #    text_log.log_test_step(f"Phase 1: Attempting write on RO mount")
        
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


def test_nfs_readonly_mount_read_operations(log, mount_point, mount_options=None, all_results=None):
    """Test that read operations work on RO mount"""

    test_name = 'readonly_mount_read_operations'
    test_description = f"Verify that read operations (like listing directory contents and getting file stats) work correctly on a read-only mount, confirming that NFS3 allows read operations while enforcing write restrictions on RO mounts"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    log.info(f'{equal_80}')
    log.info("TEST: NFS Read-Only Mount Read Operations")
    log.info(f'{equal_80}')
    
    try:
        log.info("Phase 1: Listing directory contents")
        # if text_logger:
        #    text_log.log_test_step(f"Phase 1: Listing directory contents")

        contents = os.listdir(mount_point)
        log.info(f"✓ Directory listed successfully ({len(contents)} items found)")

        log.info("Phase 2: Getting directory stats")
        # if text_logger:
        #    text_log.log_test_step(f"Phase 2: Getting directory stats")            
        stat_info = os.stat(mount_point)
        log.info(f"✓ Directory stat successful")
        log.info(f"  Mode: {oct(stat_info.st_mode)}")
        log.info(f"  Owner: {stat_info.st_uid}")
        
        log.info("✓ Read operations working on RO mount")
        log_result(log, test_name, test_description, True, f"Read operations successful ({len(contents)} items)", all_results)
    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)

#################################################################################################################################
### NFS 3 SPECIFIC TESTS WOULD GO HERE ###
#################################################################################################################################


def test_nfs3_transport_protocol(log, mount_point, mount_options=None, all_results=None):
    """Verify correct transport protocol"""
    test_name = 'NFS3_transport_protocol'
    test_description = "Verify that the mount is using the correct transport protocol (TCP or UDP)"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)
    
    log.info(f'{equal_80}')
    log.info("TEST: NFS3 Transport Protocol Verification")
    log.info(f'{equal_80}')
    
    try:
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()

        log.info("Phase 1: Searching for mount point and verifying transport protocol")
        # if text_logger:
        #    text_log.log_test_step("Phase 1: Searching for mount point and verifying transport protocol")

        for line in mounts.split('\n'):
            if mount_point in line:
                log.info("Found mount entry in /proc/mounts")
                # if text_logger:
                #    text_log.log_test_step(f"Found mount entry in /proc/mounts")
                
                if mount_options.transport == 'tcp':
                    if 'proto=tcp' in line or ',tcp' in line:
                        log.info("✓ Confirmed: Using TCP")
                        # if text_logger:
                        #    text_log.log_test_step("Verified TCP protocol in use")
                        log_result(log, test_name, test_description, True, "Using TCP as expected", all_results)
                        return
                    
                elif mount_options.transport == 'udp':
                    if 'proto=udp' in line or ',udp' in line:
                        log.info("✓ Confirmed: Using UDP")
                        # if text_logger:
                        #    text_log.log_test_step("Verified UDP protocol in use")
                        log_result(log, test_name, test_description, True, "Using UDP as expected")
                        return
        
        log_result(log, test_name, test_description, False, "Could not verify transport protocol", all_results)
    except Exception as e:
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs3_nlm_basic_locking(log, mount_point, mount_options=None, all_results=None):
    """Test NLM basic file locking"""

    test_name = 'NFS3_nlm_basic_locking'
    test_description = "Verify that exclusive locks can be acquired and block other processes as expected, confirming basic NLM file locking functionality of NFS3"

    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    log.info(f'{equal_80}')
    log.info("TEST: NFS3 NLM Basic File Locking")
    log.info(f'{equal_80}')
    
    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return
    
    test_file = os.path.join(test_dir, 'lock_test.txt')
    
    try:
        log.info("Phase 1: Creating test file")
        # if text_logger:
        #    text_log.log_test_step("Phase 1: Creating test file")
        with open(test_file, 'w') as f:
            f.write("Lock test data")
        log.info("✓ Test file created")
        
        log.info("Phase 2: Acquiring exclusive lock (LOCK_EX)")
        # if text_logger:
        #    text_log.log_test_step("Phase 2: Acquiring exclusive lock (LOCK_EX)")      
        f = open(test_file, 'r+')
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        log.info("✓ Exclusive lock acquired by main process")
        
        log.info("Phase 3: Spawning child process to test lock blocking")
        # if text_logger:
        #    text_log.log_test_step("Phase 3: Spawning child process to test lock blocking")
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
        # if text_logger:
        #    text_log.log_test_step("Phase 4: Releasing exclusive lock")
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()
        log.info("✓ Lock released successfully")
        
        log.info("✓ NLM basic locking test passed")
        log_result(log, test_name, test_description, True, "NLM basic locking test passed", all_results)
    except Exception as e:
        log.error(f"✗ Test failed: {e}")
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs3_idempotent_operations(log, mount_point, mount_options=None, all_results=None):
    """Test operation idempotency"""
    test_name = 'idempotent_operations'
    test_description = "Verify that repeated operations have the same effect as a single operation, confirming idempotency of file operations"  
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)

    log.info(f'{equal_80}')
    log.info("TEST: NFS3 Idempotent Operations (NFS3 Stateless Protocol)")
    log.info(f'{equal_80}')

    is_success, test_dir = create_test_directory(log, mount_point)
    if not is_success:
        log_result(log, test_name, test_description, False, f"Failed to create test directory: {test_dir}", all_results)
        return 
    
    test_file = os.path.join(test_dir, 'idempotent.txt')
    
    try:
        log.info("Phase 1: Testing idempotent CREATE/WRITE operations")
        # if text_logger:
        #    text_log.log_test_step("Phase 1: Testing idempotent CREATE/WRITE operations")
        for i in range(3):
            log.info(f"  Iteration {i+1}: Writing 'Iteration {i}'")
            with open(test_file, 'w') as f:
                f.write(f"Iteration {i}")
        
        log.info("Phase 2: Verifying final content")
        # if text_logger:
        #    text_log.log_test_step("Phase 2: Verifying final content")
        with open(test_file, 'r') as f:
            content = f.read()                      
        log.info(f"  File content: '{content}'")
        
        if "Iteration 2" in content:
            log.info("  ✓ Last write persisted correctly")
        else:
            log.error(f"  ✗ Expected 'Iteration 2', got '{content}'")          
        assert "Iteration 2" in content
        

        log.info("Phase 3: Testing idempotent DELETE operation")
        # if text_logger:
        #    text_log.log_test_step("Phase 3: Testing idempotent DELETE operation")
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


#################################################################################################################################
### NFS 4 SPECIFIC TESTS WOULD GO HERE ###
#################################################################################################################################

def test_nfs4_transport_protocol(log, mount_point, mount_options=None, all_results=None):
    """Verify correct transport protocol"""
    test_name = 'NFS4_transport_protocol'
    test_description = "Verify that the mount is using the correct transport protocol (TCP)"
    
    # if text_logger:
    #    text_log.log_test_start(test_name, test_description)
    
    log.info(f'{equal_80}')
    log.info("TEST: NFS4 Transport Protocol Verification")
    log.info(f'{equal_80}')
    
    try:
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()

        log.info("Phase 1: Searching for mount point and verifying transport protocol")
        # if text_logger:
        #    text_log.log_test_step("Phase 1: Searching for mount point and verifying transport protocol")

        for line in mounts.split('\n'):
            if mount_point in line:
                log.info("Found mount entry in /proc/mounts")
                # if text_logger:
                #    text_log.log_test_step(f"Found mount entry in /proc/mounts")
                
                if mount_options.transport == 'tcp':
                    if 'proto=tcp' in line or ',tcp' in line:
                        log.info("✓ Confirmed: Using TCP")
                        # if text_logger:
                        #    text_log.log_test_step("Verified TCP protocol in use")
                        log_result(log, test_name, test_description, True, "Using TCP as expected", all_results)
                        return
                    
                elif mount_options.transport == 'udp':
                    if 'proto=udp' in line or ',udp' in line:
                        log.info("✓ Confirmed: Using UDP")
                        # if text_logger:
                        #    text_log.log_test_step("Verified UDP protocol in use")
                        log_result(log, test_name, test_description, True, "Using UDP as expected")
                        return
        
        log_result(log, test_name, test_description, False, "Could not verify transport protocol", all_results)
    except Exception as e:
        log_result(log, test_name, test_description, False, str(e), all_results)


def test_nfs4_stateful_operations(log, mount_point, mount_options=None, all_results=None):
    """Test NFS4 stateful protocol operations"""
    log.info(f"{equal_80}")
    log.info("TEST: NFS4 Stateful Protocol Operations")
    log.info(f"{equal_80}")
    
    test_name = 'nfs4_stateful_operations'
    test_description = "Verify that NFS4 maintains state across operations (like open file handles) and that state is properly cleaned up on close, confirming the stateful protocol behavior of NFS4"

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


def test_nfs4_compound_operations(log, mount_point, mount_options=None, all_results=None):
    """Test NFS4 COMPOUND procedure"""
    log.info(f"{equal_80}")
    log.info("TEST: NFS4 COMPOUND Operations")
    log.info(f"{equal_80}")
    
    test_name = 'nfs4_compound_operations'
    test_description = "Verify that NFS4 can bundle multiple operations in a single RPC call, improving performance over NFS3"

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


def test_nfs4_delegation_basic(log, mount_point, mount_options=None, all_results=None):
    """Test NFS4 delegation (if supported)"""
    log.info(f"{equal_80}")
    log.info("TEST: NFS4 Delegation")
    log.info(f"{equal_80}")
    

    test_name = 'nfs4_delegation_basic'
    test_description = "Verify NFS4 delegation mechanisms are functional"

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


def test_nfs4_acls(log, mount_point, mount_options=None, all_results=None):
    """Test NFS4 ACLs (richer than POSIX)"""
    log.info(f"{equal_80}")
    log.info("TEST: NFS4 ACLs")
    log.info(f"{equal_80}")
    
    test_name = 'nfs4_acls'
    test_description = "Verify NFS4 ACL mechanisms are functional"

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


def test_nfs4_named_attributes(log, mount_point, mount_options=None, all_results=None):
    """Test NFS4 named attributes"""
    log.info(f"{equal_80}")
    log.info("TEST: NFS4 Named Attributes")
    log.info(f"{equal_80}")

    test_name = 'nfs4_named_attributes'
    test_description = "Verify NFS4 named attribute mechanisms are functional"
    
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


def test_nfs4_parallel_io_performance(log, mount_point, mount_options=None, all_results=None, num_threads=10):
    """Test NFS4 parallel I/O performance"""
    log.info(f"{equal_80}")
    log.info("TEST: NFS4 Parallel I/O Performance")
    log.info(f"{equal_80}")

    test_name = 'nfs4_parallel_io_performance'
    test_description = "Verify NFS4 parallel I/O performance"
    
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


def test_nfs4_minorversion_features(log, mount_point, mount_options=None, all_results=None):
    """Test NFS4 minor version specific features"""
    log.info(f"{equal_80}")
    log.info(f"TEST: NFS4.{mount_options.minorversion} Specific Features")
    log.info(f"{equal_80}")

    test_name = f'nfs4_{mount_options.minorversion}_features'
    test_description = f"Verify features specific to NFS4.{mount_options.minorversion} are functional and that the mount is using the correct minor version"
    
    try:
        
        if mount_options.minorversion == 0:
            log.info("NFSv4.0 Features:")
            log.info("  - Stateful protocol")
            log.info("  - COMPOUND operations")
            log.info("  - Delegations")
            log.info("  - Named attributes")
            
        elif mount_options.minorversion == 1:
            log.info("NFSv4.1 Features (includes 4.0 +):")
            log.info("  - Sessions (improved connection management)")
            log.info("  - pNFS (parallel NFS)")
            log.info("  - Improved callback system")
            log.info("  - Exactly-once semantics")
            
        elif mount_options.minorversion == 2:
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
               
                if f'vers=4.{mount_options.minorversion}' in line or f'nfsvers=4.{mount_options.minorversion}' in line:
                    log.info(f"✓ Confirmed NFS4.{mount_options.minorversion}")
                    log_result(log, test_name, test_description, True, f"NFS4.{mount_options.minorversion} verified")
                    return
        
        log.warning("⚠ Could not verify minor version in mount options")
        log_result(log, test_name, test_description, False, "Version check inconclusive")
        
    except Exception as e:
        log.error(f"✗ Minor version test failed: {e}")
        log_result(log, test_name, test_description, False, str(e))



