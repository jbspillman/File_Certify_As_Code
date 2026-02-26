import paramiko

class SSHClient:
    def __init__(self, host, username, password=None, key_path=None, port=22):
        self.host = host
        self.username = username
        self.password = password
        self.key_path = key_path
        self.port = port
        self.client = None

    def connect(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            if self.key_path:
                self.client.connect(self.host, port=self.port, username=self.username, key_filename=self.key_path)
            else:
                self.client.connect(self.host, port=self.port, username=self.username, password=self.password)
            return self
        except paramiko.AuthenticationException:
            error_string = f"✗ Authentication failed for {self.host}"
            return error_string
        except Exception as e:
            error_string = f"✗ SSH connection failed to {self.host}: {e}"
            return error_string

    def run(self, command):
        if not self.client:
            error_string = "✗ No active SSH connection"
            return None, None, error_string
        stdin, stdout, stderr = self.client.exec_command(command)
        output = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        exit_code = stdout.channel.recv_exit_status()
        return exit_code, output, error

    def disconnect(self):
        if self.client:
            self.client.close()
            self.client = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()