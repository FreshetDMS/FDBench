#!/usr/bin/env bash

DEFAULT_IOPS=100
DEFAULT_IOPS_SIZE=4096
DEFAULT_IOPS_MAX=300

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
FDBENCH_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"

if [ -z "$BASE_IMAGE" ]; then
  echo "BASE_IMAGE is not set. Exiting.."
  exit 1
fi

if [ -z "$LIBVIRT_EMULATOR" ]; then
  echo "LIBVIRT_EMULATOR is not set. Exiting.."
  exit 1
fi

if [ -z "$VMS_HOME" ]; then
  export VMS_HOME="$FDBENCH_ROOT_DIR/deploy/vms"
fi

LIBVIRT_TARGETS=/var/lib/libvirt/qemu/channel/target
SSH_KEY_DIR="$VMS_HOME/.ssh"
SSH_PVT_KEY="$SSH_KEY_DIR/key"
SSH_PUB_KEY="$SSH_KEY_DIR/key.pub"
KAFKA_VM_HOME="$VMS_HOME/kafka"
ZK_VM_HOME="$VMS_HOME/zk"

# Creating directories to store vm related files
mkdir -p "$KAFKA_VM_HOME"
mkdir -p "$ZK_VM_HOME"

KAFKA_IMAGE="$KAFKA_VM_HOME/os.img"
KAFKA_DATA_IMAGE="$KAFKA_VM_HOME/data.qcow2"
ZK_IMAGE="$ZK_VM_HOME/os.img"
KAFKA_DOMAIN_XML="$KAFKA_VM_HOME/domain.xml"
ZK_DOMAIN_XML="$ZK_VM_HOME/domain.xml"
KAFKA_IP_FILE="$KAFKA_VM_HOME/ip"
ZK_IP_FILE="$ZK_VM_HOME/ip"


BASE_IMAGE_NAME="$( basename "${BASE_IMAGE}" )"
KAFKA_DOMAIN_NAME="kafka"
ZK_DOMAIN_NAME="zk"
COMMAND=$1
KAFKA_DISK_IOPS=${2:-$DEFAULT_IOPS}
KAFKA_DISK_IOPS_SIZE=${3:-$DEFAULT_IOPS_SIZE}
KAFKA_DISK_IOPS_MAX=${4:-$DEFAULT_IOPS_MAX}

#mkdir -p $SSH_KEY_DIR

#if [ ! -f $SSH_PVT_KEY ]; then
#  ssh-keygen -t rsa -N '' -f $SSH_PVT_KEY
#fi

destroy() {
  # Shutting down VMs and cleaning domain data
  if virsh list --all | grep --quiet "kafka"
  then
    virsh shutdown "$KAFKA_DOMAIN_NAME"
    virsh destroy "$KAFKA_DOMAIN_NAME"
    virsh undefine "$KAFKA_DOMAIN_NAME"
    virsh vol-delete --pool vg0 "$KAFKA_DOMAIN_NAME.img"
  fi

  if virsh list --all | grep --quiet "zk"
  then
    virsh shutdown "$ZK_DOMAIN_NAME"
    virsh destroy "$ZK_DOMAIN_NAME"
    virsh undefine "$ZK_DOMAIN_NAME"
    virsh vol-delete --pool vg0 "$ZK_DOMAIN_NAME.img"
  fi

  # Cleaning home directories
  rm -f $KAFKA_IMAGE
  rm -f $ZK_IMAGE
  rm -f $ZK_DOMAIN_NAME
  rm -f $KAFKA_DOMAIN_XML
  rm -f $KAFKA_DATA_IMAGE
}

if [ "$COMMAND" == "destroy" ]; then
  destroy
  exit 0
elif test -z "$COMMAND"; then
  echo
  echo "  Usage.."
  echo "  $ libvirtkafkacluster.sh destroy"
  echo "  $ libvirtkafkacluster.sh provision <iops> <iops_size> <max_iops>"
  exit 1
elif [ "$COMMAND" == "provision" ]; then
  echo "Start provisioning single node Kafka cluster"
  # First cleanup any running vms
  echo "Cleaning up existing VMs"
  destroy
  echo "Done cleanup"

  # Copy OS image
  echo "Copying and creating images"
  cp "$BASE_IMAGE" "$KAFKA_IMAGE"
  cp "$BASE_IMAGE" "$ZK_IMAGE"

  # Create Kafka data disk
  qemu-img create -f qcow2 "$KAFKA_DATA_IMAGE" 100G

  echo "Creating Kafka domain"
  # Generating UUIDs for domains
  KAFKA_DOMAIN_UUID="$(uuidgen)"
  ZK_DOMAIN_UUID="$(uuidgen)"

  # Creating kafka vm domain xml
  cat > "$KAFKA_DOMAIN_XML" << EOF
<domain type='kvm'>
  <name>$KAFKA_DOMAIN_NAME</name>
  <uuid>$KAFKA_DOMAIN_UUID</uuid>
  <memory unit='KiB'>4194304</memory>
  <currentMemory unit='KiB'>4194304</currentMemory>
  <vcpu placement='static'>2</vcpu>
  <resource>
    <partition>/machine</partition>
  </resource>
  <os>
    <type arch='x86_64' machine='pc'>hvm</type>
    <boot dev='hd'/>
  </os>
  <features>
    <acpi/>
    <apic/>
  </features>
  <cpu mode='custom' match='exact'>
    <model fallback='allow'>Haswell</model>
  </cpu>
  <clock offset='utc'>
    <timer name='rtc' tickpolicy='catchup'/>
    <timer name='pit' tickpolicy='delay'/>
    <timer name='hpet' present='no'/>
  </clock>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>restart</on_reboot>
  <on_crash>restart</on_crash>
  <pm>
    <suspend-to-mem enabled='no'/>
    <suspend-to-disk enabled='no'/>
  </pm>
  <devices>
    <emulator>$LIBVIRT_EMULATOR</emulator>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='$KAFKA_IMAGE'/>
      <backingStore/>
      <target dev='vda' bus='virtio'/>
      <alias name='virtio-disk0'/>
    </disk>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='$KAFKA_DATA_IMAGE'/>
      <backingStore/>
      <target dev='vdb' bus='virtio'/>
      <alias name='virtio-disk1'/>
      <iotune>
        <total_iops_sec>$KAFKA_DISK_IOPS</total_iops_sec>
        <total_iops_sec_max>$KAFKA_DISK_IOPS_MAX</total_iops_sec_max>
        <size_iops_sec>$KAFKA_DISK_IOPS_SIZE</size_iops_sec>
      </iotune>
    </disk>
    <interface type='network'>
      <source network='default'/>
      <model type='virtio'/>
    </interface>
    <video>
      <model type='qxl'/>
    </video>
    <graphics type='spice' autoport='no' port='20001' listen='127.0.0.1'>
      <listen type='address' address='127.0.0.1'/>
    </graphics>
    <channel type="unix">
      <source mode="bind"/>
      <target type="virtio" name="org.qemu.guest_agent.0"/>
    </channel>
  </devices>
</domain>
EOF

  # Creating Kafka domain
  virsh create "$KAFKA_DOMAIN_XML"

  until virsh qemu-agent-command $KAFKA_DOMAIN_NAME '{"execute":"guest-info"}' > /dev/null 2>&1
  do
    sleep 2
  done

  KAFKA_IP="$(virsh domifaddr $KAFKA_DOMAIN_NAME | tail -2 | awk '{print $4}'| cut -d/ -f1)"

  echo $KAFKA_IP > $KAFKA_IP_FILE

# Creating kafka vm domain xml
  cat > "$ZK_DOMAIN_XML" << EOF
<domain type='kvm'>
  <name>$ZK_DOMAIN_NAME</name>
  <uuid>$ZK_DOMAIN_UUID</uuid>
  <memory unit='KiB'>4194304</memory>
  <currentMemory unit='KiB'>4194304</currentMemory>
  <vcpu placement='static'>2</vcpu>
  <resource>
    <partition>/machine</partition>
  </resource>
  <os>
    <type arch='x86_64' machine='pc'>hvm</type>
    <boot dev='hd'/>
  </os>
  <features>
    <acpi/>
    <apic/>
  </features>
  <cpu mode='custom' match='exact'>
    <model fallback='allow'>Haswell</model>
  </cpu>
  <clock offset='utc'>
    <timer name='rtc' tickpolicy='catchup'/>
    <timer name='pit' tickpolicy='delay'/>
    <timer name='hpet' present='no'/>
  </clock>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>restart</on_reboot>
  <on_crash>restart</on_crash>
  <pm>
    <suspend-to-mem enabled='no'/>
    <suspend-to-disk enabled='no'/>
  </pm>
  <devices>
    <emulator>$LIBVIRT_EMULATOR</emulator>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='$ZK_IMAGE'/>
      <backingStore/>
      <target dev='vda' bus='virtio'/>
      <alias name='virtio-disk0'/>
    </disk>
    <interface type='network'>
      <source network='default'/>
      <model type='virtio'/>
    </interface>
    <video>
      <model type='qxl'/>
    </video>
    <graphics type='spice' autoport='no' port='20002' listen='127.0.0.1'>
      <listen type='address' address='127.0.0.1'/>
    </graphics>
    <channel type="unix">
      <source mode="bind"/>
      <target type="virtio" name="org.qemu.guest_agent.0"/>
    </channel>
  </devices>
</domain>
EOF

  # Creating Kafka domain
  virsh create "$ZK_DOMAIN_XML"

  until virsh qemu-agent-command $ZK_DOMAIN_NAME '{"execute":"guest-info"}' > /dev/null 2>&1
  do
    sleep 2
  done

  ZK_IP="$(virsh domifaddr $ZK_DOMAIN_NAME | tail -2 | awk '{print $4}'| cut -d/ -f1)"

  echo $ZK_IP > $ZK_IP_FILE

  sshpass -p vagrant ssh -T -o StrictHostKeyChecking=no vagrant@$KAFKA_IP << EOF
    echo "$ZK_IP" > ~/.zkip
    sudo apt -y install linux-tools-common git
    wget http://www-us.apache.org/dist/kafka/0.10.1.0/kafka_2.11-0.10.1.0.tgz
    mkdir -p ~/.downloads
    mv kafka_2.11-0.10.1.0.tgz ~/.downloads
    tar xzf ~/.downloads/kafka_2.11-0.10.1.0.tgz -C ~/kafka
    git clone --depth 1 https://github.com/brendangregg/perf-tools
    df -h
EOF

 sshpass -p vagrant ssh -T -o StrictHostKeyChecking=no vagrant@$ZK_IP << EOF
    sudo apt -y install linux-tools-common git
    wget https://www.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
    mkdir -p ~/.downloads
    mv zookeeper-3.4.6.tar.gz ~/.downloads
    tar xzf ~/.downloads/zookeeper-3.4.6.tar.gz -C ~/zk
    git clone --depth 1 https://github.com/brendangregg/perf-tools
EOF


else
echo "Unknown command: $COMMAND. Exiting.."
exit 1
fi


