Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/xenial64"
  config.vm.hostname = "march-28-flink-docker-engine"                   # hostname of the virtual machine
  config.vm.network "forwarded_port", host: 2375, guest: 2375           # forwarding docker port to the host, don't change this!

  config.vm.synced_folder ".", "/vagrant", disabled: true
  config.vm.synced_folder "./docker-compose", "/docker-compose", owner: "root", group: "root", mount_options: ["dmode=777,fmode=777"]

  config.vm.network "forwarded_port", host: 9091, guest: 9091           # forwarding zookeeper-navigator port
  config.vm.network "forwarded_port", host: 2181, guest: 2181           # forwarding zookeeper port
  config.vm.network "forwarded_port", host: 9092, guest: 9092           # forwarding kafka port
  config.vm.network "forwarded_port", host: 9101, guest: 9101           # forwarding kafka port
  config.vm.network "forwarded_port", host: 8081, guest: 8081           # forwarding schema-registry port
  config.vm.network "forwarded_port", host: 9021, guest: 9021           # forwarding control-center port
  config.vm.network "forwarded_port", host: 8082, guest: 8082           # forwarding rest-proxy port
  config.vm.network "forwarded_port", host: 7001, guest: 7001           # forwarding job-manager-1 port
  config.vm.network "forwarded_port", host: 7002, guest: 7002           # forwarding job-manager-2 port
  config.vm.network "forwarded_port", host: 7003, guest: 7003           # forwarding job-manager-3 port
  config.vm.network "forwarded_port", host: 9000, guest: 9000           # forwarding minio port
  config.vm.network "forwarded_port", host: 9001, guest: 9001           # forwarding minio port
  config.vm.network "forwarded_port", host: 9090, guest: 9090           # forwarding prometheus port
  config.vm.network "forwarded_port", host: 3000, guest: 3000           # forwarding grafana port

  config.vm.provision "shell", path: "provision.sh"                     # the provisioning script used to install docker and expose it's port
  config.vm.provider :virtualbox do |vb|
      vb.customize [ "modifyvm", :id, "--uartmode1", "disconnected" ]   # disabling the log file generation on host
      vb.name = "march-28-flink-docker-engine"                          # virtual machine name in virtualbox
      vb.memory = 8192                                                  # your virtual machine's memory, adjust based on you need
      vb.cpus = 6                                                       # your virtual machine's cpu, adjust based on you need
  end
end
