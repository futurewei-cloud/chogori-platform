import configparser

class Runnable:
    def __init__(self):
        self.name = ""
        self.ip = ""
        self.image = ""
        self.docker_args = ""
        self.program_args = ""

    def __str__(self):
        return "Name: " + self.name + \
            "\nIP: " + self.ip + \
            "\nImage: " + self.image + \
            "\nDocker args: " + self.docker_args + \
            "\nProgram args: " + self.program_args

    def getDockerPull(self):
        return "sudo docker pull " + self.image

    def getDockerRun(self):
        return "sudo docker run " + self.docker_args + " " + self.program_args
        

def parseRunnableConfig(runnable, config_files, cpus, ip):
    binary = ""
    if cpus > 10:
        print("Warning: script does not support cpus > 10 yet!")

    for filename in config_files.split(' '):
        config = configparser.ConfigParser()
        config.read(filename)
        if "binary" in config["deployment"]:
            binary = config["deployment"]["binary"]
        if "image" in config["deployment"]:
            runnable.image = config["deployment"]["image"]
        if "docker_args" in config["deployment"]:
            runnable.docker_args += config["deployment"]["docker_args"] + " "

        for arg in config["program_args"]:
            value = config["program_args"][arg]
            if value == "$cpus":
                value = str(cpus)
            if value == "$cpus_expand":
                value = "10-" + str(10+cpus-1)
            runnable.program_args += "--" + arg + " " + value + " "

    runnable.docker_args += runnable.image + " " + binary
        

def parseConfig(filename):
    config = configparser.ConfigParser()
    config.read(filename)
    all_to_run = []

    for section in config.sections():
        ips = config[section]["ips"].split(' ')
        for ip in ips:
            runnable = Runnable()
            runnable.name = section
            runnable.ip = ip
            parseRunnableConfig(runnable, config[section]["configs"], int(config[section]["cpus"]), ip)
            all_to_run.append(runnable)

    return all_to_run
