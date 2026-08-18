from django.apps import AppConfig


class AccountsConfig(AppConfig):
    name = 'accounts'

    def ready(self):
        """Stop Django from writing last_login through the user object.

        django.contrib.auth connects update_last_login to user_logged_in, which
        calls user.save() -- and GnanaUser.save() treats a user with no
        last_login as a first-time login and sends a welcome mail through
        send_mail2('welcome_mail_from_kv.txt'). That template lives in
        service-infrastructure's mails/ directory, which neither the aviso
        wheel nor this service ships, so the first login of every user died
        with FileNotFoundError and the caller saw a 500. The second one
        worked, which is exactly the kind of thing nobody reports accurately.

        User lifecycle -- welcome mails included -- belongs to the monolith,
        not to an API service, so the signal is disconnected rather than the
        template vendored.
        """
        from django.contrib.auth.signals import user_logged_in

        user_logged_in.disconnect(dispatch_uid="update_last_login")
