#!/usr/bin/env python
import smtplib
import sys
from email.mime.text import MIMEText

SERVER = "localhost"
FROM = "noreply@archivematica.org"
TO = []  # List of email addresses
CONTENTS = """
{type} {name} ({uuid}) is waiting for user approval.

DO NOT REPLY TO THIS MESSAGE.  This email address is not monitored.

-Archivematica
"""


def main(microservice_name, first_time, unit_path, unit_uuid, unit_name, unit_type):
    if first_time != "True":
        return
    if microservice_name != "Approve normalization":
        return
    content = CONTENTS.format(name=unit_name, type=unit_type.title(), uuid=unit_uuid)
    msg = MIMEText(content)
    msg["Subject"] = f"{unit_name} waiting for user approval"
    msg["From"] = FROM
    msg["To"] = ", ".join(TO)
    s = smtplib.SMTP(SERVER)
    s.sendmail(FROM, TO, msg.as_string())
    s.quit()


if __name__ == "__main__":
    microservice_name = sys.argv[1]
    first_time = sys.argv[2]  # String True or False
    unit_path = sys.argv[3]
    unit_uuid = sys.argv[4]
    unit_name = sys.argv[5]
    unit_type = sys.argv[6]
    sys.exit(
        main(microservice_name, first_time, unit_path, unit_uuid, unit_name, unit_type)
    )
