
import pandas as pd

import pickle
from aviso.settings import EMAIL_SENDER
from aviso.utils import mailUtils
from aviso.settings import sec_context


def send_debug_email(py_obj, file_name, header, email_addresses):
    '''
    Hayley's totally dope email script. I'm stealing it and putting it here because remote debugging
    is the worst thing ever
    send_debug_email(hist_df, 'endofapply.csv', 'ENDOFAPPLY', ['matt@aviso.com', 'jimmy.ready@aviso.com'])
    '''
    if isinstance(py_obj, pd.DataFrame):
        py_obj.to_csv(file_name, compression='gzip')
    else:
        with open(file_name, 'wb') as myfile:
            pickle.dump(py_obj, myfile)
    with open(file_name, 'rb') as f:
        mailUtils.send_mail(header + " " + sec_context.details.name,
                            header + " " + sec_context.details.name,
                            EMAIL_SENDER,
                            email_addresses,
                            is_html=True,
                            attachments={file_name: f.read()},
                            mimetype='application/octet-stream'
                            )