class NoResultException(Exception):

    def __init__(self, message, params):
        self.rejection_message = message
        self.rejection_params = params
        super(NoResultException, self).__init__(message)