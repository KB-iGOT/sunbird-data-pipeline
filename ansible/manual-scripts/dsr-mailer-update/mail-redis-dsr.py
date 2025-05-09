#!/usr/bin/python3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# Email configurations
TO = """
        S.Ramadorai@tcs.com,
        secy_mop@nic.in,
        secretary@meity.gov.in,
        ceo.karmayogi@gov.in,
        chairperson-ncvet@gov.in,
        govindiyer09@gmail.com,
        debjani@nasscom.in,
        santrupt.misra@adityabirla.com,
        pankaj@peoplestrong.com,
        som.sharma@gov.in,
        shyama.roy@nic.in,
        nalini@gov.in,
        Zachariahthomas.edu@nic.in,
        pawan.joshi@nic.in,
        jitesh.gupta@nic.in,
        rakesh.verma@karmayogi.in,
        shubho.gupta@karmayogi.in,
        monojeet.chakravorty@karmayogi.in,
        ap.singh@karmayogi.in,
        priyanka.agarwal@karmayogi.in,
        bm@keenable.in,
        memberhr-cbc@gov.in,
        memberadmn.cbc-dopt@gov.in,
        chairperson-cbc@gov.in,
        rajul.bhatt@gov.in,
        anshu.aggarwal28@karmayogi.in,
        nalini.nautiyal@gmail.com,
        singhr11@ias.nic.in,
        cfo.karmayogi@gov.in,
        cto-karmayogi@gov.in,
        astha.sharma@tarento.com,
        ritesh.kumar80@karmayogi.in,
        kumar.abhinav@gov.in,
        """
BCC = """
        mathew.pallan@tarento.com,
        chandni.babu@nic.in,
        amandeep.singh@fosteringlinux.com,
        kunwara.jha@fosteringlinux.com,
        madhu.jaswal@fosteringlinux.com,
        rohtash.kumar@fosteringlinux.com,
        manoj.cchounsali@fosteringlinux.com,
        raju@fosteringlinux.com,
        igot.operations@tarento.com,
        somya.jaiswal@tarento.com,
        rangabashyam.krishnamachari@tarento.com,
        vikram.subramanyam@tarento.com,
        devendra.maurya@fosteringlinux.com,
        shobhit.vashistha@tarento.com,
        himanshu.gupta@tarento.com,
        wilky.singh@tarento.com,
        anand.varada@tarento.com,
        mohit.agarwal@tarento.com,
        vijay.sunkeswari@tarento.com,
        haridas.kakunje@tarento.com,
        spvijay@gmail.com,
        rangabashyam.pk@gmail.com,
        naushad.ali@fosteringlinux.com,
        igot.noc@tarento.com
        """
FROM_EMAIL = "mission.karmayogi@gov.in"
EMAIL_RELAY = "relay.nic.in"
EMAIL_SUBJECT = f"IGOT Mission Karmayogi - [ Automated DSR Dated ] - {datetime.now().strftime('%d/%m/%Y')}"

# Read mail body from file
with open('/var/lib/jenkins/ansible_play/aman-inventory/PMU_Report_Playbook/automate_data/Dsr-data/pmu.html', 'r') as file:
    mail_body = file.read()

# Create MIME message
message = MIMEMultipart()
message['From'] = FROM_EMAIL
message['To'] = TO
message['Bcc'] = BCC
message['Subject'] = EMAIL_SUBJECT

# Attach HTML content to the email
message.attach(MIMEText(mail_body, 'html'))

# Connect to the SMTP server and send the email
try:
    smtp_server = smtplib.SMTP(EMAIL_RELAY)
    smtp_server.send_message(message)
    smtp_server.quit()
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {e}")
