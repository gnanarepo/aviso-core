import logging
import os
import jinja2
from aviso import settings
from aviso.settings import sec_context, CNAME_DISPLAY_NAME, CNAME
from django.core.mail import EmailMultiAlternatives
from django.core.mail.message import EmailMessage


logger = logging.getLogger('gnana.%s' % __name__)


def format_datetime(value, formt='date'):
    if not value:
        return "No Date time"

    if formt == 'date':
        return value.strftime('%Y-%m-%d')
    elif formt == 'datetime':
        return value.strftime('%Y-%m-%d at %H:%M')


def aviso_support(value, include_user=False):
    value = value.split('@')
    if 'administrative.domain' in value:
        if include_user:
            return "Aviso Support (%s)" % value[0]
        else:
            return "Aviso Support"
    else:
        return value[0]


def send_mail(subject, body, sender, tolist, reply_to=None, cclist=None, bcclist=None, is_html=False,
              attachments=None, mimetype=None):
    if attachments and not is_html:
        raise Exception('Attachments allowed only with HTML mail')
    #logger.info("%s %s %s", subject, body, settings.DEBUG)
    if settings.DEBUG:
        if isinstance(tolist, (list, tuple)):
            print("This mail content is being printed on console, "
                  "since DEBUG is enabled. DEBUG should be false in production")
            print(f"Mail: {subject}")
            print(f"Sender: {sender}")
            print(f"To: {', '.join(tolist)}")
            if cclist:
                print(f"cc: {', '.join(cclist)}")
            if bcclist:
                print(f"bcc: {', '.join(bcclist)}")
            print("Body:\n")
            print(body)

            if attachments:
                for name, text in attachments.items():
                    print(f"\n\n{name}")
                    print("-" * len(name))
                    print(text)

        else:
            raise Exception('To must be a list or tuple')
    else:
        headers = None
        if reply_to:
            headers = {'reply-to': reply_to}
        try:
            if is_html:
                msg = EmailMultiAlternatives(subject, "This mail is in HTML Format. \
                                            Please enable HTML for your client to see the message", sender, tolist,
                                             cc=cclist, bcc=bcclist, headers=headers)
                msg.attach_alternative(body, "text/html")
                if attachments:
                    mime_type = mimetype if mimetype else 'text/plain'
                    for name, text in attachments.iteritems():
                        msg.attach(name, text, mime_type)
                return msg.send()
            else:
                return EmailMessage(subject, body, sender, tolist, cc=cclist, bcc=bcclist, headers=headers).send()
        except:
            logger.exception("You might not have right mail configuration, \
                            or may be your sendmail deamon is not running")


def send_mail2(fname, sender, tolist, is_html=False, **kwargs):
    reply_to = kwargs.pop('reply_to', None)
    cclist = kwargs.pop('cclist', None)
    template_engine = kwargs.pop('template_engine', 'python')
    # attachment accespted as dict {'name':'streaming text(fp.read())'}
    attachments = kwargs.pop('attachments', {})

    bcclist = ['aviso.admin@aviso.com']
    addbcc = False
    for mails in tolist:
        if mails.split('@')[1] != 'aviso.com':
            addbcc = True
    cname = settings.CNAME_DISPLAY_NAME if settings.CNAME_DISPLAY_NAME else settings.CNAME
    if settings.CNAME == 'localhost':
        kwargs['server_name'] = 'localhost'
    else:
        tenant_name = sec_context.name
        host_addition = tenant_name.split('.', 1)[0]
        host_addition = host_addition.replace('_', '-')
        kwargs['server_name'] = host_addition+"."+cname+".aviso.com"
    subject_line = None
    with open(os.path.join(settings.SITE_ROOT, 'mails', fname), 'r') as fp:
        try:
            subject_line = fp.readline()

            if template_engine == 'python':
                subject = subject_line.format(**kwargs).strip()
                body = fp.read().format(**kwargs)
            elif template_engine == 'jinja':
                jinja_env = jinja2.Environment(loader=jinja2.DictLoader({
                    'subject': subject_line,
                    'body': fp.read()
                }))
                jinja_env.filters['datetime'] = format_datetime
                jinja_env.filters['aviso_support'] = aviso_support
                subject = jinja_env.get_template("subject").render(**kwargs).strip()
                body = jinja_env.get_template("body").render(**kwargs)
            else:
                raise Exception("Unknown template '%s' engine for mails" % template_engine)

            if addbcc and settings.CNAME in ['app', 'pre-sales', 'localhost']:
                send_mail(subject, body, sender, tolist, reply_to=reply_to, cclist=cclist, bcclist=bcclist,
                          is_html=is_html, attachments=attachments)
            else:
                send_mail(subject, body, sender, tolist, reply_to=reply_to, cclist=cclist, is_html=is_html,
                          attachments=attachments)
        except Exception as e:
            logger.exception('kwargs %s - %s - subject %s' % (kwargs, e, subject_line))
            raise e
