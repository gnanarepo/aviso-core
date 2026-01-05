FROM aviso-infrastructure:latest
WORKDIR /app

COPY --chown=appuser:appuser . /app/aviso_core/

USER appuser
ENV AVISO_APPS=aviso_core
EXPOSE 8000
CMD ["gunicorn", "aviso.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3"]