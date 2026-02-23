# colorful_logger.py

from datetime import datetime
import logging
import sys
import os


def folder_setup(base_directory, uid, gid):
    if base_directory is None:
        #base_directory = os.path.dirname(os.path.abspath(__file__))
        base_directory = 'data_output'

    reports_folder = os.path.join(base_directory, 'result_reports')   
    syslog_folder = os.path.join(base_directory, 'syslog_server')


    if os.path.exists(reports_folder) and os.path.exists(syslog_folder):
        return reports_folder, syslog_folder

    if os.geteuid() == 0:  # Only attempt to set permissions if running as root
        print('Running script setup as should be done at user level, not root. Skipping folder permission setup.')
        print('Run as non-root user the first time to create folders with correct permissions, then you can run as sudo or root if needed for testing mount tests.')
        exit(1)
    else:
        ''' Create logging folders if they don't exist, and set permissions so they are writable by non-root users (since syslog capture may be running as non-root) '''

        os.makedirs(reports_folder, exist_ok=True)
        
        os.chmod(reports_folder, 0o777)
        cmd = f'chmod -R 777 {reports_folder}' 
        os.system(cmd)
        
        os.chown(reports_folder, int(uid), int(gid))
        cmd = f'chown -R {uid}:{gid} {reports_folder}'   
        os.system(cmd)


        os.makedirs(syslog_folder, exist_ok=True)
        
        os.chmod(syslog_folder, 0o777)
        cmd = f'chmod -R 777 {syslog_folder}' 
        os.system(cmd)
        
        os.chown(syslog_folder, int(uid), int(gid))
        cmd = f'chown -R {uid}:{gid} {syslog_folder}'   
        os.system(cmd)

        return reports_folder, syslog_folder


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and emojis"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m',       # Reset
        'BOLD': '\033[1m',        # Bold
        'DIM': '\033[2m',         # Dim
    }
    
    # Additional colors
    CUSTOM_COLORS = {
        'BLUE': '\033[34m',
        'CYAN': '\033[36m',
        'GREEN': '\033[32m',
        'YELLOW': '\033[33m',
        'RED': '\033[31m',
        'MAGENTA': '\033[35m',
        'BRIGHT_BLUE': '\033[94m',
        'BRIGHT_GREEN': '\033[92m',
        'BRIGHT_YELLOW': '\033[93m',
        'BRIGHT_RED': '\033[91m',
        'BRIGHT_MAGENTA': '\033[95m',
        'BRIGHT_CYAN': '\033[96m',
    }
    
    # Emojis
    EMOJIS = {
        'DEBUG': '🔍',
        'INFO': '✓',
        'WARNING': '⚠️',
        'ERROR': '✗',
        'CRITICAL': '🚨',
    }
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        emoji = self.EMOJIS.get(record.levelname, '')
        reset = self.COLORS['RESET']
        bold = self.COLORS['BOLD']
        dim = self.COLORS['DIM']
        
        timestamp = self.formatTime(record, '%Y-%m-%d %H:%M:%S')
        message = record.getMessage()
        
        # Highlight patterns
        if '[PASS]' in message:
            message = message.replace('[PASS]', f"{self.CUSTOM_COLORS['BRIGHT_GREEN']}✓ PASS{reset}")
        if '[FAIL]' in message:
            message = message.replace('[FAIL]', f"{self.CUSTOM_COLORS['BRIGHT_RED']}✗ FAIL{reset}")
        
        message = message.replace('Phase', f"{self.CUSTOM_COLORS['BRIGHT_CYAN']}{bold}Phase{reset}")
        message = message.replace('✓', f"{self.CUSTOM_COLORS['BRIGHT_GREEN']}✓{reset}")
        message = message.replace('✗', f"{self.CUSTOM_COLORS['BRIGHT_RED']}✗{reset}")
        message = message.replace('⚠', f"{self.CUSTOM_COLORS['BRIGHT_YELLOW']}⚠{reset}")
        
        if 'TEST:' in message:
            message = message.replace('TEST:', f"{self.CUSTOM_COLORS['BRIGHT_MAGENTA']}{bold}TEST:{reset}")
        
        # Highlight performance numbers
        import re
        message = re.sub(r'(\d+\.?\d*)\s*(MB/s|ops/s|ms|s\b)', 
                        rf"{self.CUSTOM_COLORS['BRIGHT_CYAN']}\1 \2{reset}", message)
        
        if record.levelname == 'INFO':
            formatted = f"{dim}{timestamp}{reset} {emoji} {message}"
        else:
            formatted = f"{dim}{timestamp}{reset} {color}{bold}[{record.levelname}]{reset} {emoji} {message}"
        
        return formatted


def setup_colorful_logger(base_folder, user_uid, user_gid, verbose: bool = False):
    """Setup logger with colors"""
    report_folder, syslog_folder = folder_setup(base_folder, user_uid, user_gid)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    # Suppress paramiko's verbose internal logging
    logging.getLogger('paramiko').setLevel(logging.CRITICAL)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    if verbose: 
        fmt = logging.Formatter('%(levelname)s | %(filename)s | %(lineno)d | %(message)s')
    else:
        fmt = ColoredFormatter()  
    console_handler.setFormatter(fmt) 
    logger.addHandler(console_handler)

    return logger, report_folder, syslog_folder


class TextDocLogger():
    """Generate simple text documentation of tests"""
    
    #def __init__(self, reports_folder: str = None, user_uid: int = 1000, user_gid: int = 1000):
    def __init__(self, reports_folder: str = None, user_uid: int = 1000, user_gid: int = 1000, output_file: str = None):        
        self.user_uid = user_uid
        self.user_gid = user_gid
        self.reports_folder = reports_folder  # ← add this
        self.output_file = output_file or f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.log_entries = []
        self.test_metadata = {}
        
    def log_metadata(self, key: str, value: str):
        """Log metadata about the test run"""
        self.test_metadata[key] = value
    
    def log_test_start(self, test_name: str, description: str):
        """Log the start of a test"""
        self.log_entries.append(
            {
                'type': 'test_start',
                'test_name': test_name,
                'description': description,
                'timestamp': datetime.now()
            }
        )
    
    def log_test_step(self, step: str):
        """Log a test step"""
        self.log_entries.append(
            {
                'type': 'step',
                'content': step,
                'timestamp': datetime.now()
            }
        )
    
    def log_test_result(self, test_name: str, passed: bool, message: str = ""):
        """Log test result"""
        self.log_entries.append(
            {
                'type': 'test_result',
                'test_name': test_name,
                'passed': passed,
                'message': message,
                'timestamp': datetime.now()
            }
        )
    
    def generate_report(self, reports_folder=None):
        
        reports_folder = reports_folder or self.reports_folder
        if os.path.isabs(self.output_file):  # full path provided, ignore reports_folder
            output_path = self.output_file
        else:
            output_path = os.path.join(reports_folder, self.output_file)

        if not reports_folder:
            raise ValueError("reports_folder must be provided either at init or when calling generate_report()")
        
        """Generate the text report"""
        report_lines = []
        
        # Header
        report_lines.append("=" * 80)
        report_lines.append("TEST NAME AND OTHER STUFF")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Metadata
        report_lines.append("TEST RUN INFORMATION")
        report_lines.append("-" * 80)
        for key, value in self.test_metadata.items():
            report_lines.append(f"{key:25}: {value}")
        report_lines.append("")
        
        # Test results
        report_lines.append("=" * 80)
        report_lines.append("TEST RESULTS AND DOCUMENTATION")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        current_test = None
        for entry in self.log_entries:
            if entry['type'] == 'test_start':
                if current_test:
                    report_lines.append("")
                
                report_lines.append("-" * 80)
                report_lines.append(f"TEST: {entry['test_name']}")
                report_lines.append("-" * 80)
                report_lines.append(f"Purpose: {entry['description']}")
                report_lines.append(f"Started: {entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                report_lines.append("")
                current_test = entry['test_name']
                
            elif entry['type'] == 'step':
                report_lines.append(f"  • {entry['content']}")
                
            elif entry['type'] == 'test_result':
                status = "PASSED ✓" if entry['passed'] else "FAILED ✗"
                report_lines.append("")
                report_lines.append(f"Result: {status}")
                if entry['message']:
                    report_lines.append(f"Details: {entry['message']}")
                report_lines.append(f"Completed: {entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                report_lines.append("")
        
        # Summary
        report_lines.append("=" * 80)
        report_lines.append("TEST SUMMARY")
        report_lines.append("=" * 80)
        
        results = [e for e in self.log_entries if e['type'] == 'test_result']
        total = len(results)
        passed = sum(1 for r in results if r['passed'])
        failed = total - passed
        
        report_lines.append(f"Total Tests: {total}")
        report_lines.append(f"Passed: {passed}")
        report_lines.append(f"Failed: {failed}")
        report_lines.append(f"Success Rate: {(passed/total*100):.1f}%" if total > 0 else "N/A")
        report_lines.append("")
        
        if failed > 0:
            report_lines.append("Failed Tests:")
            for result in results:
                if not result['passed']:
                    report_lines.append(f"  ✗ {result['test_name']}: {result['message']}")
        
        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # Write to file
        report_content = "\n".join(report_lines)
        
        # Create text_logging directory
        # output_path = os.path.join(self.reports_folder, self.output_file)  # Ensure correct path construction
        with open(output_path, 'w') as f:
            f.write(report_content)
        os.chmod(output_path, 0o777)
        os.chown(output_path, self.user_uid, self.user_gid)  # Change ownership to the user (assuming UID and GID are X)
        return output_path

