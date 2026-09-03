"""Client-side framework for the shell verbs this service ships.

preparesdk packages this directory alongside gbm_apis/sdk/ into the archive
avisosdk downloads at login, so everything here runs on the client, not on the
server: no Django, no aviso.settings, no Mongo. Third-party imports are limited
to what aviso-sdk declares (six, pytz).
"""
