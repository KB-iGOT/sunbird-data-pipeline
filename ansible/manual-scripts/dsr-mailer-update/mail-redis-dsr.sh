#!/bin/bash
MAIL=/usr/bin/swaks
TO="S.Ramadorai@tcs.com,secy_mop@nic.in,secretary@meity.gov.in,ceo.karmayogi@gov.in,chairperson-ncvet@gov.in,govindiyer09@gmail.com,debjani@nasscom.in,santrupt.misra@adityabirla.com,pankaj@peoplestrong.com,som.sharma@gov.in,shyama.roy@nic.in,nalini@gov.in,Zachariahthomas.edu@nic.in,pawan.joshi@nic.in,jitesh.gupta@nic.in,rakesh.verma@karmayogi.in,shubho.gupta@karmayogi.in,monojeet.chakravorty@karmayogi.in,ap.singh@karmayogi.in,priyanka.agarwal@karmayogi.in,bm@keenable.in,memberhr-cbc@gov.in,memberadmn.cbc-dopt@gov.in,chairperson-cbc@gov.in,rajul.bhatt@gov.in,anshu.aggarwal28@karmayogi.in,nalini.nautiyal@gmail.com,singhr11@ias.nic.in,cfo.karmayogi@gov.in,ranapratap.singh@karmayogi.in"
BCC="mathew.pallan@tarento.com,chandni.babu@nic.in,amandeep.singh@fosteringlinux.com,kunwara.jha@fosteringlinux.com,madhu.jaswal@fosteringlinux.com,rohtash.kumar@fosteringlinux.com,manoj.cchounsali@fosteringlinux.com,raju@fosteringlinux.com,igot.operations@tarento.com,somya.jaiswal@tarento.com,rangabashyam.krishnamachari@tarento.com,vikram.subramanyam@tarento.com,devendra.maurya@fosteringlinux.com,shobhit.vashistha@tarento.com,himanshu.gupta@tarento.com,wilky.singh@tarento.com,anand.varada@tarento.com,mohit.agarwal@tarento.com,vijay.sunkeswari@tarento.com,haridas.kakunje@tarento.com,spvijay@gmail.com,rangabashyam.pk@gmail.com,naushad.ali@fosteringlinux.com,astha.sharma@tarento.com,igot.noc@tarento.com"

DATE=`date +%d/%m/%Y`
MAIL_BODY=`cat /var/lib/jenkins/ansible_play/aman-inventory/PMU_Report_Playbook/automate_data/Dsr-data/pmu.html`
EMAIL_RELAY="relay.nic.in"
#################################################################################################


$MAIL \
  --to "$TO,$BCC" \
  --from "mission.karmayogi@gov.in" \
  --server "$EMAIL_RELAY" \
  --timeout 300 \
  --header "To:$TO" \
  --header "Subject: IGOT Karmayogi - [ Automated DSR Dated ] - $DATE" \
  --header "Content-Type: text/html" \
  --body "$MAIL_BODY"
