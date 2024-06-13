import os

# https://docs.python.org/3/library/email.html#module-email
# https://docs.python.org/3/library/email.examples.html


class SendMail:
    # def __init__(self):
    #     pass

    @staticmethod
    def send(to_email, subject, message_body, attachment, cc_email, bcc_email):
        recipients = to_email

        if cc_email is not None:
            recipients = ' -c {0} {1} '.format(cc_email, recipients)

        if bcc_email is not None:
            recipients = ' -b {0} {1} '.format(bcc_email, recipients)

        if attachment is not None:
            os.system('echo "{0}" | mail -s "{1}" -a "{2}" {3}'.format(message_body, subject, attachment, recipients))
        else:
            os.system('echo "{0}" | mail -s "{1}" {2}'.format(message_body, subject, recipients))
