- name: Install ${roles?join(", ")} roles
  hosts: ${ansible.hosts}
  <#if ansible.remote_user??>
  remote_user: ${ansible.remote_user}
  </#if>
  <#if ansible.become>
  become: true
  become_method: sudo
  </#if>
  <#if ansible.gfacts>
  gather_facts: yes
  <#else>
  gather_facts: no
  </#if>
  <#if vars?size gt 0>
  vars:
    <#list vars as k,v>
    ${k} : ${v}
    </#list>
  </#if>
  roles:
    <#list roles as r>
    - ${r}
    </#list>
