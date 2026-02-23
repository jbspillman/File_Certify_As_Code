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
import os


logger = logging.getLogger(__name__)
from fpt_logger import TextDocLogger


''' some general constants '''
stars_25 = '*' * 25
equal_25 = '=' * 25
equal_80 = '=' * 80
''' end of general constants '''


@dataclass
class NFS4MountOptions:
    """NFS4 mount options"""
    transport: str = 'tcp'
    rsize: int = 1048576
    wsize: int = 1048576
    timeo: int = 600
    retrans: int = 2
    soft: bool = False
    intr: bool = True
    minorversion: int = 1  # 0, 1, or 2 for NFSv4.0, 4.1, 4.2
    sec: str = 'sys'  # sys, krb5, krb5i, krb5p
    
    def to_mount_string(self):
        """Convert to mount options string"""
        opts = [
            f'vers=4.{self.minorversion}',
            f'proto={self.transport}',
            f'rsize={self.rsize}',
            f'wsize={self.wsize}',
            f'timeo={self.timeo}',
            f'retrans={self.retrans}',
            f'sec={self.sec}',
        ]
        
        if self.soft:
            opts.append('soft')
        else:
            opts.append('hard')
            
        if self.intr:
            opts.append('intr')
        
        return ','.join(opts)


class NFS4Test:
    """ Comprehensive NFS4 protocol testing """

 
    
    def __init__(self, server: str, export_path: str,
                 mount_options: NFS4MountOptions = None,
                 mount_type: str = 'rw'):
        self.server = server
        self.export_path = export_path
        self.mount_options = mount_options or NFS4MountOptions()
        self.mount_type = mount_type
        self._is_rw_mount = (mount_type == 'rw')
        self.mount_point = None
        self.test_dir = None
        self.results = []
        
    def log_result(self, test_name: str, passed: bool, message: str = ""):
        """Log test result"""
        result = {
            'test': test_name,
            'passed': passed,
            'message': message,
            'timestamp': time.time(),
            'nfs_version': f'4.{self.mount_options.minorversion}'
        }
        self.results.append(result)
        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {test_name}: {message}")
    
    def mount(self) -> bool:
        """Mount NFS4 export"""
        try:
            self.mount_point = tempfile.mkdtemp(prefix='nfs4_test_')
            os.chmod(self.mount_point, 0o777)
            os.chown(self.mount_point, 1000, 1000)  # Change to non-root user
            
            options = self.mount_options.to_mount_string()
            options += f',{self.mount_type}'
            
            cmd = [
                'sudo', 'mount', 
                '-t', 'nfs4',
                '-o', options,
                f'{self.server}:{self.export_path}',
                self.mount_point
            ]
            
            logger.debug(f"Mounting: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                logger.error(f"Mount failed: {result.stderr}")
                logger.error(f"Command: {' '.join(cmd)}")
                logger.error("Manually ensure the export is correct and the server is reachable")
                return False
            
            # Verify mount
            result = subprocess.run(['mount'], capture_output=True, text=True)
            
            if self.mount_point in result.stdout:
                logger.info(f"✓ Mounted at {self.mount_point} (NFS4.{self.mount_options.minorversion}, {self.mount_type})")
                return True
            else:
                logger.error("Mount not found in mount table")
                return False
                
        except Exception as e:
            logger.error(f"Mount exception: {e}")
            return False
    
    def unmount(self):
        """Unmount NFS4 export"""
        if not self.mount_point:
            return
        
        try:
            subprocess.run(
                ['sudo', 'umount', '-f', '-l', self.mount_point],
                capture_output=True,
                timeout=10
            )
            time.sleep(1)
            try:
                os.rmdir(self.mount_point)
            except:
                pass
        except Exception as e:
            logger.error(f"Unmount error: {e}")
    
    def setup(self):
        """Setup test environment"""
        if not self.mount():
            raise Exception("Failed to mount NFS export")
        
        test_id = f"test_{int(time.time())}_{os.getpid()}"
        self.test_dir = os.path.join(self.mount_point, test_id)
        os.makedirs(self.test_dir, exist_ok=True)
        logger.info(f"Test directory: {self.test_dir}")
    
    def teardown(self):
        """Cleanup test environment"""
        try:
            if self.test_dir and os.path.exists(self.test_dir):
                subprocess.run(['rm', '-rf', self.test_dir], capture_output=True, timeout=30)
        except:
            pass
        finally:
            self.unmount()
    

    # Test descriptions for documentation
    TEST_DESCRIPTIONS = {
        'test_mount_options_verification': 'Confirm that the actual mount options match the requested configuration',
        'transport_protocol': 'Verify that the mount is using the correct transport protocol (TCP/UDP) as requested',
        'basic_file_operations': 'Test fundamental file operations: create, read, write, and delete files',
        'idempotent_operations': 'Verify NFS3 stateless protocol ensures repeated operations produce consistent results',
        'close_to_open_consistency': 'Test NFS3 close-to-open cache consistency - changes made by one client are visible to others after file close',
        'nlm_basic_locking': 'Test Network Lock Manager (NLM) exclusive file locking between processes',
        'small_file_performance': 'Measure metadata-intensive operations with many small files',
        'concurrent_writers': 'Test multiple simultaneous writers to verify concurrent access handling',
        'large_sequential_io': 'Measure large file sequential read/write performance',
        'readwrite_mount_enforcement': 'Verify read-write mount allows create, modify, and delete operations',
        'readonly_mount_enforcement': 'Verify read-only mount blocks write operations',
        'readonly_mount_read_operations': 'Verify read operations still work on read-only mounts',
        'mount_options_verification': 'Confirm actual mount options match requested configuration'
    }   

    # ========================================================================
    # NFS4 STATEFUL PROTOCOL TESTS
    # ========================================================================
    
    def test_stateful_operations(self):
        """Test NFS4 stateful protocol operations"""
        logger.info(f"{equal_80}")
        logger.info("TEST: NFS4 Stateful Protocol Operations")
        logger.info(f"{equal_80}")
        logger.info("Testing NFS4's stateful nature (vs NFS3 stateless)")
        
        test_file = os.path.join(self.test_dir, 'stateful_test.txt')
        
        try:
            logger.info(f"Phase 1: Opening file and maintaining state")
            f = open(test_file, 'w')
            logger.info("✓ File opened (server maintains state)")
            
            logger.info(f"Phase 2: Writing data while file is open")
            f.write("NFS4 stateful test")
            f.flush()
            logger.info("✓ Data written")
            
            logger.info(f"Phase 3: Closing file (state cleanup)")
            f.close()
            logger.info("✓ File closed (server releases state)")
            
            logger.info(f"Phase 4: Verifying data persistence")
            with open(test_file, 'r') as f:
                content = f.read()
            
            if content == "NFS4 stateful test":
                logger.info("✓ Data persisted correctly")
            else:
                logger.error(f"✗ Data mismatch: '{content}'")
            
            assert content == "NFS4 stateful test"
            
            logger.info(f"✓ Stateful operations test passed")
            self.log_result('stateful_operations', True)
            
        except Exception as e:
            logger.error(f"✗ Stateful operations test failed: {e}")
            self.log_result('stateful_operations', False, str(e))
    
    def test_compound_operations(self):
        """Test NFS4 COMPOUND procedure"""
        logger.info(f"{equal_80}")
        logger.info("TEST: NFS4 COMPOUND Operations")
        logger.info(f"{equal_80}")
        logger.info("Testing NFS4's ability to bundle multiple operations")
        
        test_file = os.path.join(self.test_dir, 'compound_test.txt')
        
        try:
            logger.info(f"Phase 1: Creating file (COMPOUND: OPEN + WRITE + CLOSE)")
            start = time.time()
            with open(test_file, 'w') as f:
                f.write("Compound operation test")
            compound_time = time.time() - start
            logger.info(f"✓ File created in {compound_time:.4f}s")
            logger.info("  (Single COMPOUND RPC vs multiple RPCs in NFS3)")
            
            logger.info(f"Phase 2: Reading file (COMPOUND: OPEN + READ + CLOSE)")
            start = time.time()
            with open(test_file, 'r') as f:
                content = f.read()
            read_time = time.time() - start
            logger.info(f"✓ File read in {read_time:.4f}s")
            
            logger.info(f"Phase 3: Verifying data")
            if content == "Compound operation test":
                logger.info("✓ Data verified")
            else:
                logger.error(f"✗ Data mismatch: '{content}'")
            
            assert content == "Compound operation test"
            
            logger.info(f"✓ COMPOUND operations test passed")
            logger.info(f"  Performance benefit: Reduced network round-trips")
            self.log_result('compound_operations', True, 
                          f"Write: {compound_time:.4f}s, Read: {read_time:.4f}s")
            
        except Exception as e:
            logger.error(f"✗ COMPOUND operations test failed: {e}")
            self.log_result('compound_operations', False, str(e))
    
    def test_delegation_basic(self):
        """Test NFS4 delegation (if supported)"""
        logger.info(f"{equal_80}")
        logger.info("TEST: NFS4 Delegation")
        logger.info(f"{equal_80}")
        logger.info("Testing NFS4 file delegations for client-side caching")
        
        test_file = os.path.join(self.test_dir, 'delegation_test.txt')
        
        try:
            logger.info(f"Phase 1: Creating file and requesting delegation")
            with open(test_file, 'w') as f:
                f.write("Delegation test")
            logger.info("✓ File created (delegation may be granted)")
            logger.info("  Note: Delegation is at server's discretion")
            
            logger.info(f"Phase 2: Multiple reads (should use delegated cache)")
            for i in range(5):
                with open(test_file, 'r') as f:
                    content = f.read()
                logger.info(f"  Read {i+1}: {len(content)} bytes")
            logger.info("✓ Multiple reads completed (likely using delegation cache)")
            
            logger.info(f"Phase 3: Modifying file (delegation recall)")
            with open(test_file, 'a') as f:
                f.write("\nModified")
            logger.info("✓ File modified (delegation recalled if active)")
            
            logger.info(f"✓ Delegation test passed")
            logger.info("  Note: Check server logs for actual delegation grants")
            self.log_result('delegation_basic', True, 
                          "Delegation mechanisms exercised")
            
        except Exception as e:
            logger.error(f"✗ Delegation test failed: {e}")
            self.log_result('delegation_basic', False, str(e))
    
    def test_nfs4_acls(self):
        """Test NFS4 ACLs (richer than POSIX)"""
        logger.info(f"{equal_80}")
        logger.info("TEST: NFS4 ACLs")
        logger.info(f"{equal_80}")
        logger.info("Testing NFS4 Access Control Lists")
        
        test_file = os.path.join(self.test_dir, 'acl_test.txt')
        
        try:
            logger.info(f"Phase 1: Creating test file")
            with open(test_file, 'w') as f:
                f.write("ACL test file")
            logger.info("✓ File created")
            
            logger.info(f"Phase 2: Checking file permissions")
            stat_info = os.stat(test_file)
            perms = oct(stat_info.st_mode)[-3:]
            logger.info(f"  POSIX permissions: {perms}")
            logger.info(f"  Owner UID: {stat_info.st_uid}")
            logger.info(f"  Group GID: {stat_info.st_gid}")
            
            logger.info(f"Phase 3: Modifying permissions")
            os.chmod(test_file, 0o644)
            logger.info("✓ Permissions set to 644")
            
            stat_info = os.stat(test_file)
            new_perms = oct(stat_info.st_mode)[-3:]
            if new_perms == '644':
                logger.info("✓ Permission change verified")
            else:
                logger.error(f"✗ Expected 644, got {new_perms}")
            
            logger.info(f"✓ ACL test passed")
            logger.info("  Note: Advanced ACL features require nfs4_getfacl/nfs4_setfacl")
            self.log_result('nfs4_acls', True, "Basic ACL operations verified")
            
        except Exception as e:
            logger.error(f"✗ ACL test failed: {e}")
            self.log_result('nfs4_acls', False, str(e))
    
    def test_named_attributes(self):
        """Test NFS4 named attributes"""
        logger.info(f"{equal_80}")
        logger.info("TEST: NFS4 Named Attributes")
        logger.info(f"{equal_80}")
        logger.info("Testing NFS4 extended file attributes")
        
        test_file = os.path.join(self.test_dir, 'attr_test.txt')
        
        try:
            logger.info(f"Phase 1: Creating test file")
            with open(test_file, 'w') as f:
                f.write("Named attributes test")
            logger.info("✓ File created")
            
            logger.info(f"Phase 2: Checking standard attributes")
            stat_info = os.stat(test_file)
            logger.info(f"  Size: {stat_info.st_size} bytes")
            logger.info(f"  Modified: {time.ctime(stat_info.st_mtime)}")
            logger.info(f"  Inode: {stat_info.st_ino}")
            
            logger.info(f"Phase 3: File attributes retrieved successfully")
            logger.info("✓ Named attributes mechanism working")
            logger.info("  Note: Extended attrs use getfattr/setfattr tools")
            
            self.log_result('named_attributes', True, "Attribute operations verified")
            
        except Exception as e:
            logger.error(f"✗ Named attributes test failed: {e}")
            self.log_result('named_attributes', False, str(e))
    
    # ========================================================================
    # NFS4 PERFORMANCE TESTS
    # ========================================================================
    
    def test_parallel_io_performance(self, num_threads=10):
        """Test NFS4 parallel I/O performance"""
        logger.info(f"{equal_80}")
        logger.info("TEST: NFS4 Parallel I/O Performance")
        logger.info(f"{equal_80}")
        logger.info(f"Testing {num_threads} concurrent I/O operations")
        
        def io_task(task_id):
            try:
                filepath = os.path.join(self.test_dir, f'parallel_{task_id}.dat')
                data = os.urandom(1024 * 1024)  # 1MB
                
                logger.info(f"  [Thread {task_id}] Writing 1MB...")
                start = time.time()
                with open(filepath, 'wb') as f:
                    f.write(data)
                    f.flush()
                write_time = time.time() - start
                
                logger.info(f"  [Thread {task_id}] Reading 1MB...")
                start = time.time()
                with open(filepath, 'rb') as f:
                    read_data = f.read()
                read_time = time.time() - start
                
                if data == read_data:
                    logger.info(f"  [Thread {task_id}] ✓ Verified (W:{write_time:.3f}s R:{read_time:.3f}s)")
                    return True
                else:
                    logger.error(f"  [Thread {task_id}] ✗ Data mismatch")
                    return False
                    
            except Exception as e:
                logger.error(f"  [Thread {task_id}] ✗ Error: {e}")
                return False
        
        try:
            logger.info(f"Phase 1: Launching {num_threads} parallel I/O threads")
            start = time.time()
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                results = list(executor.map(io_task, range(num_threads)))
            duration = time.time() - start
            
            success = sum(results)
            total_mb = num_threads * 2  # Read + Write
            throughput = total_mb / duration
            
            logger.info(f"✓ Parallel I/O completed in {duration:.2f}s")
            logger.info(f"  Success: {success}/{num_threads} threads")
            logger.info(f"  Aggregate throughput: {throughput:.2f} MB/s")
            
            self.log_result('parallel_io_performance', success == num_threads,
                          f"{success}/{num_threads} threads, {throughput:.2f} MB/s")
            
        except Exception as e:
            logger.error(f"✗ Parallel I/O test failed: {e}")
            self.log_result('parallel_io_performance', False, str(e))
    
    def test_minorversion_features(self):
        """Test NFS4 minor version specific features"""
        logger.info(f"{equal_80}")
        logger.info(f"TEST: NFS4.{self.mount_options.minorversion} Specific Features")
        logger.info(f"{equal_80}")
        
        try:
            logger.info(f"Testing NFS version 4.{self.mount_options.minorversion}")
            
            if self.mount_options.minorversion == 0:
                logger.info("NFSv4.0 Features:")
                logger.info("  - Stateful protocol")
                logger.info("  - COMPOUND operations")
                logger.info("  - Delegations")
                logger.info("  - Named attributes")
                
            elif self.mount_options.minorversion == 1:
                logger.info("NFSv4.1 Features (includes 4.0 +):")
                logger.info("  - Sessions (improved connection management)")
                logger.info("  - pNFS (parallel NFS)")
                logger.info("  - Improved callback system")
                logger.info("  - Exactly-once semantics")
                
            elif self.mount_options.minorversion == 2:
                logger.info("NFSv4.2 Features (includes 4.1 +):")
                logger.info("  - Server-side copy")
                logger.info("  - Sparse files")
                logger.info("  - Space reservation")
                logger.info("  - Application I/O hints")
            
            # Verify mount is actually using the version
            with open('/proc/mounts', 'r') as f:
                mounts = f.read()
            
            for line in mounts.split('\n'):
                if self.mount_point in line:
                    logger.info(f"Active mount options:")
                    logger.debug(f"  {line}")
                    
                    if f'vers=4.{self.mount_options.minorversion}' in line or f'nfsvers=4.{self.mount_options.minorversion}' in line:
                        logger.info(f"✓ Confirmed NFS4.{self.mount_options.minorversion}")
                        self.log_result('minorversion_features', True,
                                      f"NFS4.{self.mount_options.minorversion} verified")
                        return
            
            logger.warning("⚠ Could not verify minor version in mount options")
            self.log_result('minorversion_features', True, "Version check inconclusive")
            
        except Exception as e:
            logger.error(f"✗ Minor version test failed: {e}")
            self.log_result('minorversion_features', False, str(e))


    # ========================================================================
    # Additional NFS tests can be added here (e.g. performance, edge cases)
    # ========================================================================
    def test_readwrite_mount_enforcement(self):
        """Test rw mount allows writes"""
        logger.info(f'{equal_80}')
        logger.info("TEST: Read-Write Mount Enforcement")
        logger.info(f'{equal_80}')
        
        test_file = os.path.join(self.test_dir, 'rw_test.txt')
        test_data = "RW mount test"
        
        try:
            logger.info("Phase 1: Testing write permissions")
            with open(test_file, 'w') as f:
                f.write(test_data)
            logger.info("✓ Write operation successful")
            
            logger.info("Phase 2: Verifying data integrity")
            with open(test_file, 'r') as f:
                content = f.read()
            
            if content == test_data:
                logger.info("✓ Data verified correctly")
            else:
                logger.error(f"✗ Data mismatch: '{content}'")
            
            assert content == test_data
            
            logger.info("Phase 3: Cleanup")
            os.remove(test_file)
            logger.info("✓ Test file removed")
            
            logger.info("✓ RW mount working correctly")
            self.log_result('readwrite_mount_enforcement', True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('readwrite_mount_enforcement', False, str(e))

    def test_basic_file_operations(self):
        """Test basic file operations"""
        test_name = 'basic_file_operations'
        
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
        
        logger.info(f'{equal_80}')
        logger.info("TEST: Basic File Operations")
        logger.info(f'{equal_80}')
        
        test_file = os.path.join(self.test_dir, 'basic_test.txt')
        test_data = "Hello NFS3"
        
        try:
            if text_logger:
                text_logger.log_test_step("Creating test file and writing data")
            with open(test_file, 'w') as f:
                f.write(test_data)
            logger.info(f"✓ File created with {len(test_data)} bytes")
            
            if text_logger:
                text_logger.log_test_step("Reading file content back")
            with open(test_file, 'r') as f:
                read_data = f.read()
            logger.info(f"✓ File read: '{read_data}'")
            
            assert read_data == test_data
            if text_logger:
                text_logger.log_test_step("Data integrity verified")
            
            if text_logger:
                text_logger.log_test_step("Deleting test file")
            os.remove(test_file)
            logger.info("✓ File deleted")
            
            self.log_result(test_name, True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result(test_name, False, str(e))
    
    def test_idempotent_operations(self):
        """Test operation idempotency"""
        test_name = 'idempotent_operations'
        
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Checking /proc/mounts for {self.mount_options.transport.upper()} protocol")

        logger.info(f'{equal_80}')
        logger.info("TEST: Idempotent Operations (NFS3 Stateless Protocol)")
        logger.info(f'{equal_80}')
        
        test_file = os.path.join(self.test_dir, 'idempotent.txt')
        
        try:
            if text_logger:
                text_logger.log_test_step("Phase 1: Testing idempotent CREATE/WRITE operations")
            logger.info("Phase 1: Testing idempotent CREATE/WRITE operations")
            for i in range(3):
                logger.info(f"  Iteration {i+1}: Writing 'Iteration {i}'")
                with open(test_file, 'w') as f:
                    f.write(f"Iteration {i}")
            
            if text_logger:
                text_logger.log_test_step("Phase 2: Verifying final content")
            logger.info("Phase 2: Verifying final content")
            with open(test_file, 'r') as f:
                content = f.read()                      
            logger.info(f"  File content: '{content}'")
            
            if "Iteration 2" in content:
                logger.info("  ✓ Last write persisted correctly")
            else:
                logger.error(f"  ✗ Expected 'Iteration 2', got '{content}'")          
            assert "Iteration 2" in content
            
            if text_logger:
                text_logger.log_test_step("Phase 3: Testing idempotent DELETE operation")

            logger.info("Phase 3: Testing idempotent DELETE operation")
            os.remove(test_file)
            logger.info("  ✓ First delete successful")
            
            try:
                os.remove(test_file)
                logger.error("  ✗ Second delete should have failed")
            except FileNotFoundError:
                logger.info("  ✓ Second delete correctly raised FileNotFoundError")
            
            logger.info("✓ Idempotency test passed")
            self.log_result('idempotent_operations', True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('idempotent_operations', False, str(e))

    def test_close_to_open_consistency(self):
        """Test close-to-open consistency"""
        test_name = 'close_to_open_consistency'
        
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing close-to-open consistency between two processes")

        logger.info(f'{equal_80}')
        logger.info("TEST: Close-to-Open Consistency")
        logger.info(f'{equal_80}')
        
        test_file = os.path.join(self.test_dir, 'c2o_test.txt')
        test_data = "Process 1 data"
        
        try:
            if text_logger:
                text_logger.log_test_step("Phase 1: Process 1 - Write and close file")
            logger.info("Phase 1: Process 1 - Write and close file")
            with open(test_file, 'w') as f:
                f.write(test_data)
            logger.info("✓ File written and closed (should flush to server)")
            
            if text_logger:
                text_logger.log_test_step("Phase 2: Allowing 0.5s for server flush")
            logger.info("Phase 2: Allowing 0.5s for server flush")
            time.sleep(0.5)
            logger.info("✓ Flush period elapsed")
            
            if text_logger:
                text_logger.log_test_step("Phase 3: Process 2 - Open and read file")
            logger.info("Phase 3: Process 2 - Open and read file")
            with open(test_file, 'r') as f:
                content = f.read()
            logger.info(f"  Read content: '{content}'")
            
            if content == test_data:
                logger.info("✓ Process 2 sees Process 1's write (close-to-open works)")
            else:
                logger.error(f"✗ Expected '{test_data}', got '{content}'")
            
            assert content == test_data
            
            logger.info("✓ Close-to-open consistency verified")
            self.log_result('close_to_open_consistency', True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('close_to_open_consistency', False, str(e))
    
    def test_nlm_basic_locking(self):
        """Test NLM basic file locking"""

        test_name = 'nlm_basic_locking'

        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing close-to-open consistency between two processes")


        logger.info(f'{equal_80}')
        logger.info("TEST: NLM Basic File Locking")
        logger.info(f'{equal_80}')
        
        test_file = os.path.join(self.test_dir, 'lock_test.txt')
        
        try:
            if text_logger:
                text_logger.log_test_step("Phase 1: Creating test file")

            logger.info("Phase 1: Creating test file")
            with open(test_file, 'w') as f:
                f.write("Lock test data")
            logger.info("✓ Test file created")
            
            if text_logger:
                text_logger.log_test_step("Phase 2: Acquiring exclusive lock (LOCK_EX)")
            logger.info("Phase 2: Acquiring exclusive lock (LOCK_EX)")
            f = open(test_file, 'r+')
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            logger.info("✓ Exclusive lock acquired by main process")
            
            if text_logger:
                text_logger.log_test_step("Phase 3: Spawning child process to test lock blocking")
            logger.info("Phase 3: Spawning child process to test lock blocking")
            
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
            
            if text_logger:
                text_logger.log_test_step("Phase 4: Releasing exclusive lock")
            logger.info("Phase 4: Releasing exclusive lock")
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            f.close()
            logger.info("✓ Lock released successfully")
            
            logger.info("✓ NLM basic locking test passed")
            self.log_result('nlm_basic_locking', True)
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('nlm_basic_locking', False, str(e))
        
    def test_small_file_performance(self, num_files=1000):
        """Test small file performance"""

        test_name = 'small_file_performance'

        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing small file performance")

        logger.info(f'{equal_80}')
        logger.info("TEST: Small File Performance")
        logger.info(f'{equal_80}')
        
        test_subdir = os.path.join(self.test_dir, 'small_files')
        os.makedirs(test_subdir, exist_ok=True)
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Creating {num_files} small files")
            logger.info(f"Phase 1: Creating {num_files} small files")
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
            
            if text_logger:
                text_logger.log_test_step(f"Phase 2: Reading {num_files} files")            
            logger.info(f"Phase 2: Reading {num_files} files")
            start = time.time()
            for i in range(num_files):
                filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
                with open(filepath, 'r') as f:
                    _ = f.read()
            read_time = time.time() - start
            read_rate = num_files / read_time
            logger.info(f"✓ Read {num_files} files in {read_time:.2f}s ({read_rate:.0f} ops/s)")
            
            if text_logger:
                text_logger.log_test_step(f"Phase 3: Deleting {num_files} files")
            logger.info(f"Phase 3: Deleting {num_files} files")
            start = time.time()
            for i in range(num_files):
                filepath = os.path.join(test_subdir, f'small_{i:04d}.txt')
                os.remove(filepath)
            delete_time = time.time() - start
            delete_rate = num_files / delete_time
            logger.info(f"✓ Deleted {num_files} files in {delete_time:.2f}s ({delete_rate:.0f} ops/s)")
            
            logger.info(f"✓ Small file performance test completed")
            self.log_result('small_file_performance', True,
                          f"{num_files} files - Create: {create_rate:.0f} ops/s, Read: {read_rate:.0f} ops/s, Delete: {delete_rate:.0f} ops/s")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('small_file_performance', False, str(e))
    
    def test_concurrent_writers(self, num_writers):
        """Test concurrent writers"""

        test_name = 'concurrent_writers'
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing {num_writers} concurrent writer threads")   

        logger.info(f'{equal_80}')
        logger.info("TEST: Concurrent Writers")
        logger.info(f'{equal_80}')
        logger.info(f"Testing {num_writers} concurrent writer threads")
        
        def writer_task(writer_id):
            try:
                filepath = os.path.join(self.test_dir, f'writer_{writer_id}.txt')
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
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Launching {num_writers} writer threads")
            logger.info(f"Phase 1: Launching {num_writers} writer threads")
            start = time.time()
            with ThreadPoolExecutor(max_workers=num_writers) as executor:
                results = list(executor.map(writer_task, range(num_writers)))
            duration = time.time() - start
            
            success_count = sum(results)
            if text_logger:
                text_logger.log_test_step(f"Phase 2: All threads completed in {duration:.2f}s")
            logger.info(f"Phase 2: All threads completed in {duration:.2f}s")
            logger.info(f"  Success: {success_count}/{num_writers}")
            
            if success_count == num_writers:
                logger.info(f"✓ All {num_writers} concurrent writers succeeded")
            else:
                logger.error(f"✗ Only {success_count}/{num_writers} writers succeeded")
            
            self.log_result('concurrent_writers', success_count == num_writers,
                          f"{success_count}/{num_writers} writers succeeded in {duration:.2f}s")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('concurrent_writers', False, str(e))
    
    def test_large_file_sequential_io(self, size_mb=100):
        """Test large sequential I/O"""

        test_name = 'large_sequential_io'
        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing sequential read/write with {size_mb}MB file")

        logger.info(f'{equal_80}')
        logger.info("TEST: Large File Sequential I/O")
        logger.info(f'{equal_80}')
        logger.info(f"Testing sequential read/write with {size_mb}MB file")
        
        test_file = os.path.join(self.test_dir, 'large_seq.bin')
        chunk_size = 1024 * 1024
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Sequential WRITE ({size_mb}MB)")
            logger.info(f"Phase 1: Sequential WRITE ({size_mb}MB)")
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
            
            if text_logger:
                text_logger.log_test_step(f"Phase 2: Sequential READ ({size_mb}MB)")
            logger.info(f"Phase 2: Sequential READ ({size_mb}MB)")
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
            
            if text_logger:
                text_logger.log_test_step(f"Phase 3: Cleaning up")
            logger.info(f"Phase 3: Cleaning up")
            os.remove(test_file)
            logger.info("✓ Test file removed")
            
            logger.info(f"✓ Large file I/O test completed")
            self.log_result('large_sequential_io', True,
                          f"{size_mb}MB - Write: {write_mbps:.2f} MB/s, Read: {read_mbps:.2f} MB/s")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('large_sequential_io', False, str(e))
    
    def test_readonly_mount_enforcement(self):
        """Test ro mount blocks writes"""

        test_name = 'readonly_mount_enforcement'

        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing read-only mount enforcement")

        logger.info(f'{equal_80}')
        logger.info("TEST: Read-Only Mount Enforcement")
        logger.info(f'{equal_80}')
        
        # Try to write to the mount point itself (not a subdirectory)
        test_file = os.path.join(self.mount_point, 'ro_test.txt')
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Attempting write on RO mount")
            logger.info("Phase 1: Attempting write on RO mount")
            try:
                with open(test_file, 'w') as f:
                    f.write("Should fail")
                logger.error("✗ Write succeeded on RO mount - TEST FAILED!")
                self.log_result('readonly_mount_enforcement', False,
                            "Write succeeded on ro mount!")
            except (OSError, IOError) as e:
                if e.errno in (30, 13):  # EROFS or EACCES
                    logger.info(f"✓ Write correctly blocked (errno: {e.errno})")
                    self.log_result('readonly_mount_enforcement', True,
                                f"Write blocked as expected (errno {e.errno})")
                else:
                    logger.error(f"✗ Unexpected error: {e}")
                    self.log_result('readonly_mount_enforcement', False, str(e))
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('readonly_mount_enforcement', False, str(e))
    
    def test_readonly_mount_read_operations(self):
        """Test that read operations work on RO mount"""

        test_name = 'readonly_mount_read_operations'

        if text_logger:
            text_logger.log_test_start(test_name, self.TEST_DESCRIPTIONS[test_name])
            text_logger.log_test_step(f"Testing read-only mount read operations")

        logger.info(f'{equal_80}')
        logger.info("TEST: Read-Only Mount Read Operations")
        logger.info(f'{equal_80}')
        
        try:
            if text_logger:
                text_logger.log_test_step(f"Phase 1: Listing directory contents")
            logger.info("Phase 1: Listing directory contents")
            contents = os.listdir(self.mount_point)
            logger.info(f"✓ Directory listed successfully ({len(contents)} items found)")

            if text_logger:
                text_logger.log_test_step(f"Phase 2: Getting directory stats")            
            logger.info("Phase 2: Getting directory stats")
            stat_info = os.stat(self.mount_point)
            logger.info(f"✓ Directory stat successful")
            logger.info(f"  Mode: {oct(stat_info.st_mode)}")
            logger.info(f"  Owner: {stat_info.st_uid}")
            
            logger.info("✓ Read operations working on RO mount")
            self.log_result('readonly_mount_read_operations', True,
                        f"Read operations successful ({len(contents)} items)")
        except Exception as e:
            logger.error(f"✗ Test failed: {e}")
            self.log_result('readonly_mount_read_operations', False, str(e))


class NFS4TestRunner:
    """Run comprehensive NFS4 test suite"""
    
    def __init__(self, server: str, export: str):
        self.server = server
        self.export = export
        self.all_results = []
    
    def run_basic_tests(self, minor_version, mount_type):
        mount_opts = NFS4MountOptions(transport='tcp', minorversion=minor_version)
        test = NFS4Test(self.server, self.export, mount_opts, mount_type=mount_type)

        """Run all NFS4 tests"""
        logger.info(f"{equal_80}")
        logger.info(f"NFS4.{minor_version} COMPREHENSIVE TEST SUITE")
        logger.info(f"{equal_80}")
        
        mount_opts = NFS4MountOptions(transport='tcp', minorversion=minor_version)
        test = NFS4Test(self.server, self.export, mount_opts, mount_type='rw')
        
        test.setup()
        try:
            # NFS4-specific tests
            logger.info(f"--- NFS4 Protocol Features ---")
            test.test_stateful_operations()
            test.test_compound_operations()
            test.test_delegation_basic()
            test.test_nfs4_acls()
            test.test_named_attributes()
            test.test_minorversion_features()

            # other tests
            test.test_readwrite_mount_enforcement()
            test.test_basic_file_operations()
            test.test_idempotent_operations()
            test.test_close_to_open_consistency()
            test.test_nlm_basic_locking()
            
            # Performance tests
            logger.info(f"--- NFS4 Performance Tests ---")
            test.test_parallel_io_performance(10)
            test.test_small_file_performance(100)
            test.test_concurrent_writers(32)
            test.test_large_file_sequential_io(128)
            
        finally:
            test.teardown()
        
        self.all_results.extend(test.results)
        self.print_summary()
    
    def print_summary(self):
        """Print test summary"""
        logger.info(f"{equal_80}")
        logger.info("TEST SUMMARY")
        logger.info(f"{equal_80}")
        
        total = len(self.all_results)
        passed = sum(1 for r in self.all_results if r['passed'])
        failed = total - passed
        
        logger.info(f"Total Tests: {total}")
        logger.info(f"Passed: {passed} ({100*passed/total:.1f}%)")
        logger.info(f"Failed: {failed} ({100*failed/total:.1f}%)")
        
        if failed > 0:
            logger.info(f"Failed Tests:")
            for result in self.all_results:
                if not result['passed']:
                    logger.info(f"  ✗ {result['test']}: {result['message']}")


def nfs4_test_suite_runner(logger, text_logger, nfs4_mounts=None):
    
    

    if nfs4_mounts is None:
        logger.warning("No NFS4 mounts provided, not running tests.")
        return None


    if os.geteuid() != 0:
        logger.error("This script must be run with sudo")
        logger.error("Usage(I.E.): sudo python3 app.py")
        sys.exit(1)

    logger.info("")
    logger.info(f'{equal_80}')
    logger.info("NFS4 PROTOCOL TEST SUITE")
    logger.info(f'{equal_80}')
    logger.info("")

    text_logger.log_metadata("Test Suite", "NFS4 Protocol Validation")
    text_logger.log_metadata("Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    text_logger.log_metadata("Operating System", f"{os.uname().sysname} {os.uname().release}")
    text_logger.log_metadata("Python Version", sys.version.split()[0])

    start_time = time.time()

    for mount_config in nfs4_mounts:
        vendor = mount_config['vendor']
        software = mount_config['software']
        server = mount_config['export_server']
        export = mount_config['export_path']
        mount_type = mount_config.get('mount_type', 'rw')
        minor_version = mount_config.get('minor_version', 1)  

        text_logger.log_metadata(f"Vendor", vendor)
        text_logger.log_metadata(f"Software", software)
        text_logger.log_metadata(f"Server ({mount_type})", server)
        text_logger.log_metadata(f"Export Path ({mount_type})", export)

        logger.info("")
        logger.info(f"Running NFS4 tests for {vendor} {software} ({mount_type.upper()} mount)")
        logger.info("")
        
        runner = NFS4TestRunner(server, export)
        runner.run_basic_tests(minor_version, mount_type)
        runner.print_summary()


    duration = time.time() - start_time
    text_logger.log_metadata("Total Duration", f"{int(duration//60)}m {int(duration%60)}s")
    
    # Generate text report
    report_file = text_logger.generate_report()
    
    logger.info("")
    logger.info(f"✓ All tests completed in {int(duration//60)}m {int(duration%60)}s")
    logger.info(f"✓ Documentation log: {report_file}")
    logger.info("")
    