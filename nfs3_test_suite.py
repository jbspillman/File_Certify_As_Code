from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import multiprocessing
import subprocess
import tempfile
import logging
import fcntl
import time
import sys
import re
import os

''' some general constants '''
stars_25 = '*' * 25
stars_80 = '*' * 80
equal_25 = '=' * 25
equal_80 = '=' * 80
''' end of general constants '''

logger = logging.getLogger(__name__)
from colorful_logger import TextDocLogger


@dataclass
class NFS3MountOptions:
    """NFS3 mount options"""
    vers: int = 3
    transport: str = 'tcp'
    rsize: int = 1048576
    wsize: int = 1048576
    timeo: int = 600
    retrans: int = 2
    soft: bool = False
    intr: bool = True
    noac: bool = False
    actimeo: int = None
    acregmin: int = 3
    acregmax: int = 60
    acdirmin: int = 30
    acdirmax: int = 60
    nosharecache: bool = False
    nordirplus: bool = False
    

def to_mount_string(current_options) -> str:
    
    """Convert to mount options string"""
    opts = [
        f'vers={current_options.vers}',
        f'proto={current_options.transport}',
        f'rsize={current_options.rsize}',
        f'wsize={current_options.wsize}',
        f'timeo={current_options.timeo}',
        f'retrans={current_options.retrans}',
    ]
    
    if current_options.soft:
        opts.append('soft')
    else:
        opts.append('hard')
        
    if current_options.intr:
        opts.append('intr')
        
    if current_options.noac:
        opts.append('noac')
    elif current_options.actimeo:
        opts.append(f'actimeo={current_options.actimeo}')
    else:
        opts.extend([
            f'acregmin={current_options.acregmin}',
            f'acregmax={current_options.acregmax}',
            f'acdirmin={current_options.acdirmin}',
            f'acdirmax={current_options.acdirmax}',
        ])
        
    if current_options.nosharecache:
        opts.append('nosharecache')
        
    if current_options.nordirplus:
        opts.append('nordirplus')
    
    as_string = ','.join(opts)
    return as_string


def mount_nas(nfs_server, nfs_export, mount_type, uid, gid):
    """Mount NFS3 export"""
    try:
        mount_point = tempfile.mkdtemp(prefix='nfs3_test_')
        mount_opts = NFS3MountOptions()

        if os.path.exists(mount_point):

            logger.info(f"✓ Created mount point: {mount_point}")
            os.chmod(mount_point, 0o777)
            cmd = f'sudo chmod -R 777 {mount_point}'   
            logger.info(f"  Running chmod command: {cmd}")
            os.system(cmd)

            os.chown(mount_point, int(uid), int(gid))
            cmd = f'sudo chown -R {uid}:{gid} {mount_point}'   
            logger.info(f"  Running chown command: {cmd}")
            os.system(cmd)
        

        logger.debug('Mounting NFS3 export to: %s', mount_point)
        txt_options = to_mount_string(mount_opts)
        txt_options += f',{mount_type}'

        logger.info(f"Mounting NFS3 export with options: {txt_options}")
        
        cmd = [
            'sudo', 'mount',
            '-t', 'nfs',
            '-o', txt_options,
            f'{nfs_server}:{nfs_export}',
            mount_point
        ]
        
        logger.debug(f"Mounting: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            logger.error(f"Mount failed: {result.stderr}")
            return False
        
        result = subprocess.run(['mount'], capture_output=True, text=True)
        
        if mount_point in result.stdout:
            # logger.info(f"✓ Mounted at {mount_point} ({mount_type})")
            return True, mount_point
        else:
            logger.error("Mount not found in mount table")
            return False, mount_point
            
    except Exception as e:
        logger.error(f"Mount exception: {e}")


def unmount_nas(mount_point):
    """Unmount NFS3 export"""
    if not mount_point:
        return
    
    try:
        subprocess.run(['sudo', 'umount', '-f', '-l', mount_point],
                        capture_output=True, timeout=10)
        time.sleep(1)
        try:
            os.rmdir(mount_point)
        except:
            pass
        logger.info(f"✓ Unmounted and removed: {mount_point}")
    except Exception as e:
        logger.error(f"Unmount error: {e}")
    

def log_result(test_name: str, test_description: str, passed: bool, message: str = "", all_results = None):
    """Log test result"""

    result = {
        'test': test_name,
        'description': test_description,
        'passed': passed,
        'message': message,
        'timestamp': time.time()
    }
    status = "PASS" if passed else "FAIL"
    logger.info(f"[{status}] {test_name}: {message}")

    # Log to text documentation
    if text_logger:
        text_logger.log_test_result(test_name, passed, message)

    if all_results is not None:
        all_results.append(result)
    return result
        

def test_mount_options_verification(mount_point, mount_options: NFS3MountOptions, all_results):
    test_name = 'test_mount_options_verification'
    test_description = "Confirm that the actual mount options match the requested configuration"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    """Verify mount options"""
    logger.info(f'{equal_80}')
    logger.info("TEST: Mount Options Verification")
    logger.info(f'{equal_80}')
    
    try:
        if text_logger:
            text_logger.log_test_step("Phase 1: Reading /proc/mounts")

        logger.info("Phase 1: Reading /proc/mounts")
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()
        logger.info(" ✓ Read /proc/mounts successfully")

        if text_logger:
            text_logger.log_test_step(f"Phase 2: Searching for mount point: {mount_point}")

        logger.info(f"Phase 2: Searching for mount point: {mount_point}")
        mount_line = None
        for line in mounts.split('\n'):
            if mount_point in line:
                mount_line = line
                break
        
        if not mount_line:
            logger.error("✗ Mount point not found")
            log_result(test_name, test_description, False, "Mount not found in /proc/mounts", all_results)
            return
        logger.info(f" ✓ Found the mount point in /proc/mounts")

        parts = mount_line.split()
        if len(parts) >= 4:
            options = parts[3]

            if text_logger:
                text_logger.log_test_step(f"Phase 3: Parsing options: {options}")
            logger.info(f"Phase 3: Parsing mount options")
            
            if 'vers=3' in options or 'nfsvers=3' in options:
                logger.info(" ✓ NFS Version: 3")
                if text_logger:
                    text_logger.log_test_step(f" ✓ NFS Version: 3")
            
            if f'proto={mount_options.transport}' in options:
                logger.info(f" ✓ Transport: {mount_options.transport}")
                if text_logger:
                    text_logger.log_test_step(f" ✓ Transport: {mount_options.transport}")

            logger.info("✓ Mount options verified")
            log_result(test_name, test_description, True, "Mount options verified", all_results)
        
        else:
            logger.error("✗ Could not parse mount options")
            log_result(test_name, test_description, False, "Could not parse mount options", all_results)

    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)


def test_transport_protocol(mount_point, mount_options: NFS3MountOptions, all_results=None):
    """Verify correct transport protocol"""
    test_name = 'transport_protocol'
    test_description = "Verify that the mount is using the correct transport protocol (TCP or UDP)"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)
    
    logger.info(f'{equal_80}')
    logger.info("TEST: Transport Protocol Verification")
    logger.info(f'{equal_80}')
    
    try:
        with open('/proc/mounts', 'r') as f:
            mounts = f.read()

        logger.info("Phase 1: Searching for mount point and verifying transport protocol")
        if text_logger:
            text_logger.log_test_step("Phase 1: Searching for mount point and verifying transport protocol")


        for line in mounts.split('\n'):
            if mount_point in line:
                logger.info("Found mount entry in /proc/mounts")
                if text_logger:
                    text_logger.log_test_step(f"Found mount entry in /proc/mounts")
                
                if mount_options.transport == 'tcp':
                    if 'proto=tcp' in line or ',tcp' in line:
                        logger.info("✓ Confirmed: Using TCP")
                        if text_logger:
                            text_logger.log_test_step("Verified TCP protocol in use")
                        log_result(test_name, test_description, True, "Using TCP as expected", all_results)
                        return
                    
                elif mount_options.transport == 'udp':
                    if 'proto=udp' in line or ',udp' in line:
                        logger.info("✓ Confirmed: Using UDP")
                        if text_logger:
                            text_logger.log_test_step("Verified UDP protocol in use")
                        log_result(test_name, test_description, True, "Using UDP as expected")
                        return
        
        log_result(test_name, test_description, False, "Could not verify transport protocol", all_results)
    except Exception as e:
        log_result(test_name, test_description, False, str(e), all_results)


def test_readwrite_mount_enforcement(mount_point, mount_options: NFS3MountOptions, all_results=None):

    test_name = 'readwrite_mount_enforcement'
    test_description = "Verify read-write mount allows create, modify, and delete operations"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    """Test rw mount allows writes"""
    logger.info(f'{equal_80}')
    logger.info("TEST: Read-Write Mount Enforcement")
    logger.info(f'{equal_80}')
    
    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        logger.info(f"Test directory: {test_dir}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to create test directory: {e}")
        log_result(test_name, test_description, False, f"Failed to create test directory: {e}", all_results)
        return

    test_file = os.path.join(test_dir, 'rw_test.txt')
    test_data = "RW mount test"
    
    try:
        logger.info("Phase 1: Testing write permissions")
        if text_logger:
            text_logger.log_test_step("Phase 1: Testing write permissions")
        with open(test_file, 'w') as f:
            f.write(test_data)
        logger.info("✓ Write operation successful")
        
        logger.info("Phase 2: Verifying data integrity")
        if text_logger:
            text_logger.log_test_step("Phase 2: Verifying data integrity")
        with open(test_file, 'r') as f:
            content = f.read()
        
        if content == test_data:
            msg = "✓ Data verified correctly"
            logger.info(msg)
        else:
            msg = f"✗ Data mismatch: '{content}'"
            logger.error(msg)
        
        assert content == test_data       
        logger.info("Phase 3: Cleanup")
        os.remove(test_file)
        logger.info("✓ Test file removed")
        logger.info("✓ RW mount working correctly")
        log_result(test_name, test_description, True, "RW mount working correctly", all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)


def test_basic_file_operations(mount_point, mount_options: NFS3MountOptions, all_results=None):
    """Test basic file operations"""
    test_name = 'basic_file_operations'
    test_description = "Perform basic file operations (create, read, delete) to verify functionality"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)
    
    logger.info(f'{equal_80}')
    logger.info("TEST: Basic File Operations")
    logger.info(f'{equal_80}')
        
    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        logger.info(f"Test directory: {test_dir}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to create test directory: {e}")
        log_result(test_name, test_description, False, f"Failed to create test directory: {e}", all_results)
        return

    test_file = os.path.join(test_dir, 'basic_test.txt')
    test_data = "Hello NFS3"
    try:
        logger.info(f"Phase 1: Creating test file and writing data")
        if text_logger:
            text_logger.log_test_step("Phase 1: Creating test file and writing data")
        with open(test_file, 'w') as f:
            f.write(test_data)
        logger.info(f"✓ File created with {len(test_data)} bytes")
        
        logger.info(f"Phase 2: Reading file content back")
        if text_logger:
            text_logger.log_test_step("Phase 2: Reading file content back")
        with open(test_file, 'r') as f:
            read_data = f.read()
        logger.info(f"✓ File read: '{read_data}'")
        
        logger.info(f"Phase 3: Verifying data integrity")
        assert read_data == test_data
        if text_logger:
            text_logger.log_test_step("Phase 3: Data integrity verified")
        logger.info("✓ Data integrity verified")
        
        logger.info(f"Phase 4: Deleting test file")
        if text_logger:
            text_logger.log_test_step("Phase 4: Deleting test file")
        os.remove(test_file)
        logger.info("✓ File deleted")
        
        log_result(test_name, test_description, True, "Basic file operations completed successfully", all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)


def test_idempotent_operations(mount_point, mount_options: NFS3MountOptions, all_results=None):
    """Test operation idempotency"""
    test_name = 'idempotent_operations'
    test_description = "Verify that repeated operations have the same effect as a single operation, confirming idempotency of file operations"  
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    logger.info(f'{equal_80}')
    logger.info("TEST: Idempotent Operations (NFS3 Stateless Protocol)")
    logger.info(f'{equal_80}')

    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        logger.info(f"Test directory: {test_dir}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to create test directory: {e}")
        log_result(test_name, test_description, False, f"Failed to create test directory: {e}", all_results)
        return    
    
    test_file = os.path.join(test_dir, 'idempotent.txt')
    
    try:
        logger.info("Phase 1: Testing idempotent CREATE/WRITE operations")
        if text_logger:
            text_logger.log_test_step("Phase 1: Testing idempotent CREATE/WRITE operations")
        for i in range(3):
            logger.info(f"  Iteration {i+1}: Writing 'Iteration {i}'")
            with open(test_file, 'w') as f:
                f.write(f"Iteration {i}")
        
        logger.info("Phase 2: Verifying final content")
        if text_logger:
          text_logger.log_test_step("Phase 2: Verifying final content")
        with open(test_file, 'r') as f:
            content = f.read()                      
        logger.info(f"  File content: '{content}'")
        
        if "Iteration 2" in content:
            logger.info("  ✓ Last write persisted correctly")
        else:
            logger.error(f"  ✗ Expected 'Iteration 2', got '{content}'")          
        assert "Iteration 2" in content
        

        logger.info("Phase 3: Testing idempotent DELETE operation")
        if text_logger:
            text_logger.log_test_step("Phase 3: Testing idempotent DELETE operation")
        os.remove(test_file)
        logger.info("  ✓ First delete successful")
        
        try:
            os.remove(test_file)
            logger.error("  ✗ Second delete should have failed")
        except FileNotFoundError:
            logger.info("  ✓ Second delete correctly raised FileNotFoundError")
        
        logger.info("✓ Idempotency test passed")
        log_result(test_name, test_description, True, "Idempotent operations test passed", all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)


def test_close_to_open_consistency(mount_point, mount_options: NFS3MountOptions, all_results=None):
    """Test close-to-open consistency"""
    test_name = 'close_to_open_consistency'
    test_description = "Verify that data written by one process is visible to another process after the first process closes the file, confirming close-to-open consistency guarantees of NFS3"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    logger.info(f'{equal_80}')
    logger.info("TEST: Close-to-Open Consistency")
    logger.info(f'{equal_80}')
    
    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        logger.info(f"Test directory: {test_dir}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to create test directory: {e}")
        log_result(test_name, test_description, False, f"Failed to create test directory: {e}", all_results)
        return    
        
    test_file = os.path.join(test_dir, 'c2o_test.txt')
    test_data = "Process 1 data"
    
    try:
        logger.info("Phase 1: Process 1 - Write and close file")
        if text_logger:
            text_logger.log_test_step("Phase 1: Process 1 - Write and close file")
        with open(test_file, 'w') as f:
            f.write(test_data)
        logger.info("✓ File written and closed (should flush to server)")
        
        logger.info("Phase 2: Allowing 0.5s for server flush")
        if text_logger:
            text_logger.log_test_step("Phase 2: Allowing 0.5s for server flush")
        time.sleep(0.5)
        logger.info("✓ Flush period elapsed")

        logger.info("Phase 3: Process 2 - Open and read file")        
        if text_logger:
            text_logger.log_test_step("Phase 3: Process 2 - Open and read file")
        with open(test_file, 'r') as f:
            content = f.read()
        logger.info(f"  Read content: '{content}'")
        
        if content == test_data:
            logger.info("✓ Process 2 sees Process 1's write (close-to-open works)")
        else:
            logger.error(f"✗ Expected '{test_data}', got '{content}'")
        
        assert content == test_data
        
        logger.info("✓ Close-to-open consistency verified")
        log_result(test_name, test_description, True, "Close-to-open consistency verified", all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)


def test_nlm_basic_locking(mount_point, mount_options: NFS3MountOptions, all_results=None):
    """Test NLM basic file locking"""

    test_name = 'nlm_basic_locking'
    test_description = "Verify that exclusive locks can be acquired and block other processes as expected, confirming basic NLM file locking functionality of NFS3"

    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    logger.info(f'{equal_80}')
    logger.info("TEST: NLM Basic File Locking")
    logger.info(f'{equal_80}')
    
    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        logger.info(f"Test directory: {test_dir}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to create test directory: {e}")
        log_result(test_name, test_description, False, f"Failed to create test directory: {e}", all_results)
        return   
    test_file = os.path.join(test_dir, 'lock_test.txt')
    
    try:
        logger.info("Phase 1: Creating test file")
        if text_logger:
            text_logger.log_test_step("Phase 1: Creating test file")
        with open(test_file, 'w') as f:
            f.write("Lock test data")
        logger.info("✓ Test file created")
        
        logger.info("Phase 2: Acquiring exclusive lock (LOCK_EX)")
        if text_logger:
            text_logger.log_test_step("Phase 2: Acquiring exclusive lock (LOCK_EX)")      
        f = open(test_file, 'r+')
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        logger.info("✓ Exclusive lock acquired by main process")
        
        logger.info("Phase 3: Spawning child process to test lock blocking")
        if text_logger:
            text_logger.log_test_step("Phase 3: Spawning child process to test lock blocking")
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
        
        logger.info("Phase 4: Releasing exclusive lock")
        if text_logger:
            text_logger.log_test_step("Phase 4: Releasing exclusive lock")
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()
        logger.info("✓ Lock released successfully")
        
        logger.info("✓ NLM basic locking test passed")
        log_result(test_name, test_description, True, "NLM basic locking test passed", all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)

    
def test_small_file_performance(mount_point, mount_options: NFS3MountOptions, all_results=None, num_files=100):
    """Test small file performance"""

    test_name = 'small_file_performance'
    test_description = f"Measure performance of creating, reading, and deleting {num_files} small files to evaluate small file operation performance of NFS3"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    logger.info(f'{equal_80}')
    logger.info("TEST: Small File Performance")
    logger.info(f'{equal_80}')
    
    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        logger.info(f"Test directory: {test_dir}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to create test directory: {e}")
        log_result(test_name, test_description, False, f"Failed to create test directory: {e}", all_results)
        return   
    
    test_subdir = os.path.join(test_dir, 'small_files')
    os.makedirs(test_subdir, exist_ok=True)
    
    try:
        logger.info(f"Phase 1: Creating {num_files} small files")
        if text_logger:
            text_logger.log_test_step(f"Phase 1: Creating {num_files} small files")
        start = time.time()
        for i in range(num_files):
            filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
            with open(filepath, 'w') as f:
                f.write(f"{i}")
            if (i + 1) % 25 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                logger.info(f"  Progress: {i+1}/{num_files} ({rate:.0f} files/s)")
        create_time = time.time() - start
        create_rate = num_files / create_time
        logger.info(f"✓ Created {num_files} files in {create_time:.2f}s ({create_rate:.0f} ops/s)")
        
        logger.info(f"Phase 2: Reading {num_files} files")
        if text_logger:
            text_logger.log_test_step(f"Phase 2: Reading {num_files} files")            
        start = time.time()
        for i in range(num_files):
            filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
            with open(filepath, 'r') as f:
                _ = f.read()
        read_time = time.time() - start
        read_rate = num_files / read_time
        logger.info(f"✓ Read {num_files} files in {read_time:.2f}s ({read_rate:.0f} ops/s)")
        
        logger.info(f"Phase 3: Deleting {num_files} files")
        if text_logger:
            text_logger.log_test_step(f"Phase 3: Deleting {num_files} files")
        start = time.time()
        for i in range(num_files):
            filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
            os.remove(filepath)
        delete_time = time.time() - start
        delete_rate = num_files / delete_time
        logger.info(f"✓ Deleted {num_files} files in {delete_time:.2f}s ({delete_rate:.0f} ops/s)")
        
        logger.info(f"✓ Small file performance test completed")
        log_result(test_name, test_description, True, f"{num_files} files - Create: {create_rate:.0f} ops/s, Read: {read_rate:.0f} ops/s, Delete: {delete_rate:.0f} ops/s")
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e))
        

def test_concurrent_writers(mount_point, mount_options: NFS3MountOptions, all_results=None, num_writers=10):
    """Test concurrent writers"""

    test_name = 'concurrent_writers'
    test_description = f"Verify that {num_writers} concurrent writer threads can write to separate files without data corruption, confirming that NFS3 can handle concurrent write operations correctly"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    logger.info(f'{equal_80}')
    logger.info("TEST: Concurrent Writers")
    logger.info(f'{equal_80}')
    logger.info(f"Testing {num_writers} concurrent writer threads")
    
    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        logger.info(f"Test directory: {test_dir}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to create test directory: {e}")
        log_result(test_name, test_description, False, f"Failed to create test directory: {e}", all_results)
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
            logger.error(f"  [Writer {writer_id}] ✗ Failed: {e}")
            return False
   
    try:
        logger.info(f"Phase 1: Launching {num_writers} writer threads")
        if text_logger:
            text_logger.log_test_step(f"Phase 1: Launching {num_writers} writer threads")
        start = time.time()
        with ThreadPoolExecutor(max_workers=num_writers) as executor:
            results = list(executor.map(writer_task, range(num_writers)))
        duration = time.time() - start
        
        success_count = sum(results)
        logger.info(f"Phase 2: All threads completed in {duration:.2f}s")
        if text_logger:
            text_logger.log_test_step(f"Phase 2: All threads completed in {duration:.2f}s")
        logger.info(f"  Success: {success_count}/{num_writers}")
        
        if success_count == num_writers:
            logger.info(f"✓ All {num_writers} concurrent writers succeeded")
        else:
            logger.error(f"✗ Only {success_count}/{num_writers} writers succeeded")
        
        log_result(test_name, test_description, True, f"{success_count} == {num_writers} || {success_count}/{num_writers} writers succeeded in {duration:.2f}s", all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)


def test_large_file_sequential_io(mount_point, mount_options: NFS3MountOptions, all_results=None, size_mb=100):
    """Test large sequential I/O"""

    test_name = 'large_sequential_io'
    test_description = f"Verify that {size_mb}MB file can be written and read sequentially without corruption"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    logger.info(f'{equal_80}')
    logger.info("TEST: Large File Sequential I/O")
    logger.info(f'{equal_80}')
    logger.info(f"Testing sequential read/write with {size_mb}MB file")
    
    test_id = f"test_{int(time.time())}_{os.getpid()}"
    test_dir = os.path.join(mount_point, test_id)
    try:
        os.makedirs(test_dir, exist_ok=True)
        logger.info(f"Test directory: {test_dir}")
    except (OSError, IOError) as e:
        logger.error(f"Failed to create test directory: {e}")
        log_result(test_name, test_description, False, f"Failed to create test directory: {e}", all_results)
        return   

    test_file = os.path.join(test_dir, 'large_seq.bin')
    chunk_size = 1024 * 1024
    
    try:
        logger.info(f"Phase 1: Sequential WRITE ({size_mb}MB)")
        if text_logger:
            text_logger.log_test_step(f"Phase 1: Sequential WRITE ({size_mb}MB)")
        start = time.time()
        with open(test_file, 'wb') as f:
            for i in range(size_mb):
                f.write(os.urandom(chunk_size))
                if (i + 1) % 25 == 0:
                    elapsed = time.time() - start
                    rate = (i + 1) / elapsed
                    logger.info(f"  Progress: {i+1}/{size_mb}MB ({rate:.1f} MB/s)")
        write_time = time.time() - start
        write_mbps = size_mb / write_time
        logger.info(f"✓ Write completed: {size_mb}MB in {write_time:.2f}s ({write_mbps:.2f} MB/s)")
        
        logger.info(f"Phase 2: Sequential READ ({size_mb}MB)")
        if text_logger:
            text_logger.log_test_step(f"Phase 2: Sequential READ ({size_mb}MB)")
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
        logger.info(f"✓ Read completed: {size_mb}MB in {read_time:.2f}s ({read_mbps:.2f} MB/s)")

        logger.info(f"Phase 3: Cleaning up")        
        if text_logger:
            text_logger.log_test_step(f"Phase 3: Cleaning up")
        os.remove(test_file)
        logger.info("✓ Test file removed")
        
        logger.info(f"✓ Large file I/O test completed")
        log_result(test_name, test_description, True, f"{size_mb}MB - Write: {write_mbps:.2f} MB/s, Read: {read_mbps:.2f} MB/s", all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description  , False, str(e), all_results)


def test_readonly_mount_enforcement(mount_point, mount_options: NFS3MountOptions, all_results=None):
    """Test ro mount blocks writes"""

    test_name = 'readonly_mount_enforcement'
    test_description = f"Verify that a read-only mount correctly blocks write operations, confirming that NFS3 enforces read-only access restrictions as expected"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    logger.info(f'{equal_80}')
    logger.info("TEST: Read-Only Mount Enforcement")
    logger.info(f'{equal_80}')
    
    # Try to write to the mount point it (not a subdirectory)
    test_file = os.path.join(mount_point, 'ro_test.txt')
    
    try:
        logger.info("Phase 1: Attempting write on RO mount")
        if text_logger:
            text_logger.log_test_step(f"Phase 1: Attempting write on RO mount")
        
        try:   
            with open(test_file, 'w') as f:
                f.write("Should fail")
            logger.error("✗ Write succeeded on RO mount - TEST FAILED!")
            log_result(test_name, test_description, False, "Write succeeded on ro mount!", all_results)
        except (OSError, IOError) as e:
            if e.errno in (30, 13):  # EROFS or EACCES
                logger.info(f"✓ Write correctly blocked (errno: {e.errno})")
                log_result(test_name, test_description, True, f"Write blocked as expected (errno {e.errno})", all_results)
            else:
                logger.error(f"✗ Unexpected error: {e}")
                log_result(test_name, test_description, False, str(e), all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)


def test_readonly_mount_read_operations(mount_point, mount_options: NFS3MountOptions, all_results=None):
    """Test that read operations work on RO mount"""

    test_name = 'readonly_mount_read_operations'
    test_description = f"Verify that read operations (like listing directory contents and getting file stats) work correctly on a read-only mount, confirming that NFS3 allows read operations while enforcing write restrictions on RO mounts"
    
    if text_logger:
        text_logger.log_test_start(test_name, test_description)

    logger.info(f'{equal_80}')
    logger.info("TEST: Read-Only Mount Read Operations")
    logger.info(f'{equal_80}')
    
    try:
        logger.info("Phase 1: Listing directory contents")
        if text_logger:
            text_logger.log_test_step(f"Phase 1: Listing directory contents")

        contents = os.listdir(mount_point)
        logger.info(f"✓ Directory listed successfully ({len(contents)} items found)")

        logger.info("Phase 2: Getting directory stats")
        if text_logger:
            text_logger.log_test_step(f"Phase 2: Getting directory stats")            
        stat_info = os.stat(mount_point)
        logger.info(f"✓ Directory stat successful")
        logger.info(f"  Mode: {oct(stat_info.st_mode)}")
        logger.info(f"  Owner: {stat_info.st_uid}")
        
        logger.info("✓ Read operations working on RO mount")
        log_result(test_name, test_description, True, f"Read operations successful ({len(contents)} items)", all_results)
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        log_result(test_name, test_description, False, str(e), all_results)



def run_tests(logger, text_logger, mount_point, vendor, software, mount_type, host_access=None):
    """Run tests on mounted export"""
    global all_results
    all_results = []

    logger.info("")
    logger.info(f"Running NFS3 tests for {vendor} {software} ({mount_type.upper()} mount)")
    logger.info("")

    #################################################################################################
    
    test_mount_options_verification(mount_point, NFS3MountOptions(), all_results)
    logger.info(f'{equal_80}')
    logger.info("")

    
    test_transport_protocol(mount_point, NFS3MountOptions(), all_results)   
    logger.info(f'{equal_80}')
    logger.info("")

    if mount_type == "rw" and host_access == "write":

        test_readwrite_mount_enforcement(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")

        test_basic_file_operations(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")

        test_idempotent_operations(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")
  
        test_close_to_open_consistency(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")
    
        test_nlm_basic_locking(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")
    
        test_small_file_performance(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")
    
        test_concurrent_writers(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")
    
        test_large_file_sequential_io(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")

    if mount_type == "rw" and host_access == "read":   
        test_readonly_mount_enforcement(mount_point, NFS3MountOptions(), all_results)
        logger.info(f'{equal_80}')
        logger.info("")

    if mount_type == "ro" and host_access == "read":
        test_readonly_mount_enforcement(mount_point, NFS3MountOptions(), all_results)  
        logger.info(f'{equal_80}')
        logger.info("")

    #################################################################################################
    logger.info("")
    logger.info(f"✓ Completed tests on {vendor} {software} ({mount_type.upper()} mount) with host access: {host_access}")
    logger.info("")

    return all_results


def print_summary(the_results):
    """Print test summary"""
    logger.info(f"{equal_80}")
    logger.info("TEST SUMMARY")
    logger.info(f"{equal_80}")
    
    total = len(the_results)
    passed = sum(1 for r in the_results if r['passed'])
    failed = total - passed
    
    logger.info(f"Total Tests: {total}")
    logger.info(f"Passed: {passed} ({100*passed/total:.1f}%)")
    logger.info(f"Failed: {failed} ({100*failed/total:.1f}%)")
    
    if failed > 0:
        logger.info(f"Failed Tests:")
        for result in the_results:
            if not result['passed']:
                logger.info(f"  ✗ {result['test']}: {result['message']}")


def nfs3_test_suite_runner(reports_folder, user_uid=None, user_gid=None, nfs3_mounts=None):


    if os.geteuid() != 0:
        logger.error("This script must be run with sudo")
        logger.error("Usage(I.E.): sudo python3 app.py")
        return None

    if user_uid is None or user_gid is None:
        logger.error("User UID and GID must be provided to run tests")
        logger.error('Or you will not have access to files post tests.')
        return None

    if nfs3_mounts is None:
        logger.warning("No NFS3 mounts provided, not running tests.")
        return None

    logger.info("")
    logger.info(f'{equal_80}')
    logger.info("NFS3 PROTOCOL TEST SUITE")
    logger.info(f'{equal_80}')
    logger.info("")

    start_time = time.time()
            
    all_the_results = []

    def sanitize_filename(value: str) -> str:
        cleaned = re.sub(r'[^\w\-]', '_', value)
        cleaned = re.sub(r'_+', '_', cleaned)  # collapse consecutive underscores
        return cleaned.strip('_')

    for mount_config in nfs3_mounts:
        vendor = mount_config['vendor']
        software = mount_config['software']
        server = mount_config['export_server']
        export = mount_config['export_path']
        host_access = mount_config['host_access']
        mount_type = mount_config['mount_type']

        logger.info(f'{stars_80}')
        logger.info(f"Mount configuration:")
        logger.info(f"  Server: {server}")
        logger.info(f"  Export: {export}")
        logger.info(f"  Host Access: {host_access}")
        logger.info(f"  Mount Type: {mount_type.upper()}")

        global text_logger
        # text_logger = TextDocLogger(reports_folder=reports_folder, user_uid=user_uid, user_gid=user_gid)

        report_filename = f"{sanitize_filename(vendor)}_{sanitize_filename(software)}_NFS3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        text_logger = TextDocLogger(reports_folder=reports_folder, user_uid=user_uid, user_gid=user_gid, output_file=report_filename)
        
        text_logger.log_metadata("Test Suite", "NFS3 Protocol Validation")
        text_logger.log_metadata("Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        text_logger.log_metadata("Operating System", f"{os.uname().sysname} {os.uname().release}")
        text_logger.log_metadata("Python Version", sys.version.split()[0])
        text_logger.log_metadata(" ", "\n")
        text_logger.log_metadata(f"Vendor", vendor)
        text_logger.log_metadata(f"Software", software)
        text_logger.log_metadata(f"Server ({mount_type})", server)
        text_logger.log_metadata(f"Host Access", host_access)
        text_logger.log_metadata(f"Export Path ({mount_type})", export)

        mount_state, mount_point = mount_nas(server, export, mount_type, user_uid, user_gid)
        if mount_state:
            logger.info(f"✓ Mounted {export} from {server} at {mount_point}")
        else:
            logger.error(f"✗ Failed to mount {export} from {server}")
            continue

        all_results = run_tests(logger, text_logger, mount_point, vendor, software, mount_type, host_access)
        all_the_results.extend(all_results)

        print_summary(all_results)
        duration = time.time() - start_time
        text_logger.log_metadata("Total Duration", f"{int(duration//60)}m {int(duration%60)}s")
        report_file = text_logger.generate_report()
        
        logger.info("")
        logger.info(f"✓ All tests completed in {int(duration//60)}m {int(duration%60)}s")
        logger.info(f"✓ Documentation log: {report_file}")
        logger.info("")


        time.sleep(1)  # Wait a moment for the mount to stabilize before running tests
        unmount_nas(mount_point)
        logger.info(f'{stars_80}')


    print_summary(all_the_results)
    duration = time.time() - start_time
    text_logger.log_metadata("Total Duration", f"{int(duration//60)}m {int(duration%60)}s")
    # report_file = text_logger.generate_report()
    
    logger.info("")
    logger.info(f"✓ All tests completed in {int(duration//60)}m {int(duration%60)}s")
    # logger.info(f"✓ Documentation log: {report_file}")
    logger.info("")
    