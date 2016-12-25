- name: Create EC2 instances for ${ec2.tag}
  hosts: ${ansible.hosts}
  <#if ansible.connection??>
  connection: ${ansible.connection}
  </#if>
  <#if ansible.gfacts>
  gather_facts: yes
  <#else>
  gather_facts: no
  </#if>
  tasks:
    - name: Launch instances
      ec2:
        key_name: ${ec2.keypair_id}
        group: ${ec2.security_group}
        instance_type: ${ec2.instance_type}
        image: ${ec2.image}
        wait: yes
        region: ${ec2.region}
        count: ${ec2.instances}
        <#if ec2.spot_price??>
        spot_price: ${ec2.spot_price}
        spot_wait_timeout: 600
        </#if>
        <#if ec2.ebs_volumes??>
        volumes:
          <#list ec2.ebs_volumes as v>
           - device_name: ${v.name}
             volume_type: ${v.type}
             iops: ${v.iops}
             volume_size: ${v.size}
             delete_on_termination: ${v.delete_on_termination}
          </#list>
        </#if>
        register: ${ec2.node_group}
    - name: Add new instances to a hosts group
      add_host: name={{ item.1.public_dns_name }} groups=${ec2.host_group} id={{ item.0 + 1 }} private_ip={{ item.1.private_ip }} instance_type={{ item.1.instance_type }}
      with_indexed_items: ${ec2.node_group}.instances
    - name: Wait for SSH to come up
      wait_for: hosts={{ item.public_dns_name }} port=22 delay=60 timeout=360 state=started
      with_items: ${ec2.node_group}.instances
    - name: Make sure the known hosts file exists
      file: "path={{ ssh_known_hosts_file }} state=touch"
    - name: Check hosts name availability
      shell: "ssh-keygen -f {{ ssh_known_hosts_file }} -F {{ item.public_dns_name }}"
      with_items: ${ec2.node_group}.instances
      register: ${ec2.tag}_ssh_known_host_results
      ignore_errors: yes
    - name: Scan the public key
      shell: "{{ ssh_known_hosts_command}} {{ item.item.public_dns_name }} >> {{ ssh_known_hosts_file }}"
      with_items: ${ec2.tag}_ssh_known_host_results.results
      when: item.stdout == ""