'''
MIT License

Copyright (c) 2021 Futurewei Cloud

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import configparser


class Runnable:
    def __init__(self):
        # Following values are computed by clusterstate.py dynamically based on the rest of the system state
        self.host = ""
        self.program_args = ""
        self.cpus = []
        # Following values are computed by parse.py and the static config files
        self.name = ""
        self.image = ""
        self.docker_args = ""
        self.component = ""
        self.num_cpus = 0
        self.program_args_list = []
        self.target_config = ""

    def __str__(self):
        return "Name: " + self.name + \
               "\nHost: " + self.host + \
               "\nImage: " + self.image + \
               "\nDocker args: " + self.docker_args + \
               "\nProgram args: " + self.program_args

    def MakeProgramArgsString(self):
        for arg in self.program_args_list:
            self.program_args += f"--{arg[0]} {arg[1]} "

    def getDockerPull(self):
        return "sudo docker pull " + self.image

    def getDockerRun(self):
        return "sudo docker run " + self.docker_args + " " + self.program_args

    def getDockerStop(self):
        return "sudo docker stop -t 30 " + self.name

    def getDockerRemove(self):
        return "sudo docker container rm " + self.name

    def getDockerLogs(self):
        return "sudo docker logs --tail 5000 " + self.name


def parseRunnableConfig(runnable, config_files, cpus):
    binary = ""

    parsed_args = []
    for filename in config_files.split(' '):
        config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        config.read([filename])

        if "binary" in config["deployment"]:
            binary = config["deployment"]["binary"]
        if "image" in config["deployment"]:
            runnable.image = config["deployment"]["image"]
        if "docker_args" in config["deployment"]:
            runnable.docker_args += config["deployment"]["docker_args"] + " "

        for arg in config["program_args"]:
            if arg in parsed_args:
                continue

            parsed_args.append(arg)
            value = config["program_args"][arg]
            if value == "$cpus":
                value = str(cpus)
            runnable.program_args_list.append([arg, value])

    runnable.docker_args += "--name " + runnable.name + " " + runnable.image + " " + binary
    runnable.num_cpus = cpus


def parseConfig(config_filename):
    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    config.read([config_filename])
    all_to_run = []

    for section in config.sections():
        if section == "LocalConfig":
            continue

        for i in range(0, int(config[section]["hosts"])):
            runnable = Runnable()
            runnable.component = section
            runnable.name = f"{section}{i}"
            runnable.target_config = config[section]["host_config"]
            cpus = int(config[section]["cpus"])
            parseRunnableConfig(runnable, config[section]["configs"], cpus)
            all_to_run.append(runnable)

    return all_to_run
