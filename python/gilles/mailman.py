import os
import smtplib

from email.mime.application import MIMEApplication
from email.MIMEImage import MIMEImage
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


class Mailman(object):
    def __init__(self, sender, server, user, password):
        self.sender = sender
        self.server = server
        self.user = user
        self.password = password
        # self.lookup = TemplateLookup(
                # directories = blah,
                # format_exceptions = True,
                # output_encoding = "utf-8"

    def send_content(self, subject, receivers, html, images=[]):
        # t = self.lookup.get_template(template_name)
        # t.output_encoding = "utf-8"
        # html = t.render_unicode(**params)
        wrapper = MIMEMultipart()
        wrapper["From"] = self.sender
        wrapper["Subject"] = subject
        wrapper["To"] = ", ".join(receivers)
        msg = MIMEText(html, "html")
        wrapper.attach(msg)
        for img in images:
            with open(img, "r") as f_in:
                img_part = MIMEApplication(f_in.read(),
                                           Name=os.path.basename(filename))
                img_part.add_header("Content-Disposition", "attachment",
                                    filename=os.path.basename(img))
                wrapper.attach(img_part)
        s = smtplib.SMTP_SSL(self.server)
        s.login(self.user, self.password)
        s.sendmail(self.sender, receivers, wrapper.as_string())

