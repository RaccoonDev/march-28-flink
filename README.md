## Using Vagrant to replace Docker Desktop for macOS users

1- Uninstall Docker Desktop

    Open Docker Desktop, select Docker menu, select Troubleshoot, select Uninstall

2- Install Brew from https://brew.sh

    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"


3- Install VirtualBox, Vagrant and Docker

    brew install virtualbox vagrant docker

3- Start the vagrant virtual machine by running command below

    vagrant up

4- Add line below to your .zshrc file in you home directory

    export DOCKER_HOST="tcp://0.0.0.0:2375"

5- Open a new terminal and you should be able to use docker like you used to do before.

    docker-compose up -d

8- These commands are useful when working with this setup

    vagrant up          # to start your virtual machine
    vagrant suspend     # to suspend your virtual machine
    vagrant reload      # to restart your virtual machine, do it when changin cpu or memory configs in Vagrantfile for instance
    vagrant destroy     # to remove the virtual machine

9- The Vagrantfile is also commented, have a look there if you need more info


## Stream Processing Playground

This repository contains a playground for learning stream processin via Flink.

This playground contains the following:
- zookeeper
- kafka broker
- confluent schema registry
- confluent control center
- kafka rest proxy
- battle.net server emulator (generates test events)
- prometheus
- grafana
- flink
- minio - as s3 storage

To run the platform do the following:
1. Start confluent platform: `docker-compose up -d`
1. Start minio s3: `docker-compose --profile s3 up -d` <-- make sure to add a bucket called flink to minio before starting flink
1. Start flink: `docker-compose --profile flink up -d`
1. Start prometheus and grafana: `docker-compose --profile monitoring up -d`
1. Start everything: `docker-compose --profile s3 --profile monitoring --profile flink up -d`

Useful links:
- Confluent Control Center: http://127.0.0.1:9021
- Minio UI: http://127.0.0.1:9001
- Flink UI: http://127.0.0.1:7001
- Grafana: http://127.0.0.1:3000
- Prometheus UI: http://127.0.0.1:9090
