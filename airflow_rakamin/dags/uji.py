import subprocess

def run_ssis():
    package_path = "C:\\Users\\lenym\\source\\repos\\Integration Services Project1\\Integration Services Project1\\Package.dtsx"
    # Sesuaikan dengan path dan opsi yang sesuai untuk dtexec
    command = f'dtexec /F "{package_path}"'

    # Menjalankan command menggunakan subprocess
    subprocess.run(command)

run_ssis()
