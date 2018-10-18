# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = '2'

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = 'bento/ubuntu-16.04'

  config.vm.define 'africa' do |africa|
  end

  config.vm.network 'forwarded_port', guest: 8000, host: 8000

  config.vm.network 'private_network', ip: '192.168.20.17'

  config.vm.provider 'virtualbox' do |provider|
    provider.customize ['modifyvm', :id, '--memory', '1024']
    provider.name = 'africa'
  end

  config.vm.provision 'ansible' do |ansible|
    ansible.playbook = '.vagrant_provisioning/playbook.yml'
    # ansible.tags = ''
    # ansible.verbose = 'vvv'
  end
end