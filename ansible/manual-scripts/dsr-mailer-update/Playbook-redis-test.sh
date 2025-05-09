rm -rf /var/lib/jenkins/ansible_play/aman-inventory/PMU_Report_Playbook/automate_data/Dsr-data/script-logs.txt
rm -rf  /var/lib/jenkins/ansible_play/aman-inventory/PMU_Report_Playbook/automate_data/Dsr-data/pmu.html

cd /var/lib/jenkins/ansible_play/aman-inventory/PMU_Report_Playbook/automate_data/Dsr-data

/usr/bin/ansible-playbook -i inventory  main-redis.yml

#/usr/bin/ansible-playbook mail.yml
#bash mail-redis-dsr.sh
python3 mail-redis-dsr.py
