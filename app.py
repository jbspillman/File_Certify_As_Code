from colorful_logger import setup_colorful_logger
from nfs3_test_suite import nfs3_test_suite_runner
from syslog_manager import start_syslog_server, stop_syslog_server
from audit_test_suite import audit_configure, generate_netapp_audit_events
import time
import os






def main():
    base_folder = "data"   # Base folder for all output, logs, and test results
    user_uid = 1000        # Set this to the UID of the user that will be running the tests, used for chowning mounted directories to allow write access for testing    
    user_gid = 1000        # Set this to the GID of the user that will be running the tests, used for chowning mounted directories to allow write access for testing

    logger, report_folder, syslog_folder = setup_colorful_logger(base_folder, user_uid, user_gid, verbose=True)
    logger.info(f"Report folder: {report_folder}")
    logger.info(f"Syslog folder: {syslog_folder}")  

    ''' Storage Appliances with NFS3 exports to test against, to be used in the nfs3_test_suite_runner '''
    nfs3_mounts = [
        {
            'vendor': 'Dell',
            'software': 'PowerScale OneFS 9.10.0.0',
            'export_server': 'onefs002-2.beastmode.local.net',
            'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_rw',
            'host_access': 'write',  # what access level the client is supposed to have. Should align to export-policy rules.
            'mount_type': 'rw'       # mount as read-write, even if you don't have access.
        },
        {
            'vendor': 'Dell',
            'software': 'PowerScale OneFS 9.10.0.0',
            'export_server': 'onefs002-2.beastmode.local.net',
            'export_path': '/ifs/ACCESS_ZONES/system/nfs3_01_ro',  # this export should be read-only according to export policy rules, but we will mount it as read-write to test enforcement.
            'host_access': 'read',  # what access level the client is supposed to have. Should align to export-policy rules.
            'mount_type': 'rw'      # mount as read-write, even if you don't have access.
        },
        {
            'vendor': 'NetApp',
            'software': 'ONTAP 9.16.1P1',
            'export_server': 'svm01.beastmode.local.net',
            'export_path': '/svm01_vol_nfs30_1_rw',
            'host_access': 'write',
            'mount_type': 'rw'
        },
        {
            'vendor': 'NetApp',
            'software': 'ONTAP 9.16.1P1',
            'export_server': 'svm01.beastmode.local.net',
            'export_path': '/svm01_vol_nfs30_0_ro',
            'host_access': 'read',
            'mount_type': 'ro'
        },
    ]

    ''' Storage Appliances with NFS4 exports to test against, to be used in the nfs4_test_suite_runner '''
    nfs4_mounts = [
        {
            'vendor': 'NetApp',
            'software': 'ONTAP 9.16.1P1',
            'export_server': 'svm01.beastmode.local.net',
            'export_path': '/svm01_vol01',
            'mount_type': 'rw',
            'minor_version': 0   # 0=NFSv4.0, 1=NFSv4.1, 2=NFSv4.2
        },
        {
            'vendor': 'NetApp',
            'software': 'ONTAP 9.16.1P1',
            'export_server': 'svm01.beastmode.local.net',
            'export_path': '/svm01_vol02',
            'mount_type': 'rw',
            'minor_version': 1   # 0=NFSv4.0, 1=NFSv4.1, 2=NFSv4.2
        },  
        {
            'vendor': 'NetApp',
            'software': 'ONTAP 9.16.1P1',
            'export_server': 'svm01.beastmode.local.net',
            'export_path': '/svm01_vol02',
            'mount_type': 'rw',
            'minor_version': 2   # 0=NFSv4.0, 1=NFSv4.1, 2=NFSv4.2
        } 
    ]

    ''' Storage Appliances to test Audit logging against, to be used in the audit_test_suite_runner '''
    storage_appliances = [
        # {
        #     'vendor': 'Dell',
        #     'software': 'PowerScale OneFS 9.10.0.0',
        #     'management_device': 'onefs002-2.beastmode.local.net',
        #     'management_username': 'root',
        #     'management_password':  'Onefs123',
        #     "syslog_server": "beastserver.beastmode.local.net",
        #     "syslog_port": 55555,
        #     "syslog_protocol": "udp"
        # },
        {
            'vendor': 'NetApp',
            'software': 'ONTAP 9.16.1',
            'management_device': 'ontap001-mgmt.beastmode.local.net',
            'management_username': 'admin',
            'management_password':  'Netapp123',
            "syslog_server": "beastserver.beastmode.local.net",
            "syslog_port": 55555,
            "syslog_protocol": "udp"
        }
    ]
    
    os.system('clear')  # Clear the console screen

    for appliance in storage_appliances:
        logger.info(f"Planned Storage Appliance for testing: {appliance['vendor']} {appliance['software']} @ {appliance['management_device']}")


        # START SYSLOG CAPTURE SERVER #
        syslog_udp, syslog_tcp, syslog_capture, syslog_file_path = start_syslog_server(syslog_folder, port=55555, verbose=False, silent=True)    
        logger.info(f"Syslog capture server started, logging to: {syslog_file_path}")    
 
        # CONFIGURE AUDIT SETTINGS ON STORAGE APPLIANCES #
        audit_result = audit_configure(logger, appliance, audit_action='Set Audit Settings')
        logger.info(f"Audit settings configured on {appliance['vendor']} {appliance['software']} @ {appliance['management_device']} | Result: {audit_result}")
        
        audit_commands = generate_netapp_audit_events(logger, appliance)
      
        time.sleep(5)  # wait a bit to ensure the logs are captured before we


        # ================= PROTOCOL TEST SUITES  ================= #
        vendor_nfs3_mounts = []
        for mount in nfs3_mounts:
            if mount['vendor'] == appliance['vendor']:
                vendor_nfs3_mounts.append(mount)

        # NFS3 TESTS #
        nfs3_test_suite_runner(report_folder, user_uid, user_gid, vendor_nfs3_mounts)


        time.sleep(5)  # wait a bit to ensure the logs are captured before we

        # REMOVE AUDIT SETTINGS FROM STORAGE APPLIANCES #
        audit_result = audit_configure(logger, appliance, audit_action='Delete Audit Settings')
        logger.info(f"Audit settings unconfigured on {appliance['vendor']} {appliance['software']} @ {appliance['management_device']} | Result: {audit_result}")

   
    # STOP SYSLOG CAPTURE SERVER #
    stop_syslog_server(syslog_udp, syslog_tcp, syslog_capture, syslog_file_path)        
    logger.info(f"Syslog capture server stopped, logged to: {syslog_file_path}")




if __name__ == "__main__":
    main()

    







