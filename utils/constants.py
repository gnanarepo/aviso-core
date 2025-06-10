from aviso.settings import CNAME


ALGOLIA_INDEX_NAME_FOR_DEAL_RECORDS = 'deal_records'
APPLICATION_JSON = 'application/json'
MEETID_NOT_ALLOWED_CHARS = r'[^:_a-zA-Z0-9]'
MEET_FILE_NAME_NOT_ALLOWED_CHARS = r'[^:.-_a-zA-Z0-9@!#$%&*()<>?/|}{~]'
EXTERNAL = 'external'
FILE_PATH_FORMAT = '{tenant}/{meeting_type}'
MEETINGS_BUCKET_NAME = ('collaboration-meetings-prod'
                        if CNAME == 'app' else
                        'collaboration-meetings-qa')
PRIVATE_MOXTRA_URL = "https://avisomeet.aviso.com/web/websdk/dist/mepsdk.js?ver=7.16.6"
PUBLIC_MOXTRA_URL = "https://aviso-dev.moxtra.com/web/websdk/dist/mepsdk.js?ver=7.16.6"

VERSION_CONFIGURABLE_PRIVATE_MOXTRA_URL = "https://avisomeet.aviso.com/web/websdk/dist/mepsdk.js?ver={sdk_version}"
VERSION_CONFIGURABLE_PUBLIC_MOXTRA_URL = "https://aviso-dev.moxtra.com/web/websdk/dist/mepsdk.js?ver={sdk_version}"

VALID = '_'
doc360URL = 'https://identity.document360.io/jwt/generateCode'
doc360AuthURL = 'https://dochelp.aviso.com/jwt/authorize?code='
doc360username = '46e44d8b-8e4d-4d53-8bf6-74728c66e765'
doc360secrect = 'oEYfwwvRppeIE7tuyoCAolGxp61h1h6U0JML4HkrC80'

FERNET_SECRET_KEY = "FeYaD7iCSS7z2PC8lvECgiyvjFS40HHtk4ebH6JxBHU="

FILTER_TABLE_SEARCH_SCHEMA = [
   {
        "index": "Calls",
        "filter_fields":
        {
            'order': ['participants',
                    'words',
                    'trackers',
                    'date',
                    'call_duration',
                    'deal_name',
                    'account_name',
                    'web_conference',
                    'buyer_interest_score',
                    'talk_ratio',
                    'interactivity_ratio',
                    'longest_monologue'],
            "schema": [
                {
                    "domain": [

                    ],
                    "key": "participants.name",
                    "label": "Call Participants",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "words",
                    "label": "Themes",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [
                    ],
                    "key": "trackers",
                    "label": "Keywords",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "date",
                    "label": "Date",
                    "type": "date",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "call_duration",
                    "label": "Call Duration",
                    "type": "slider",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "deal_name",
                    "label": "Deal Name",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "account_name",
                    "label": "Account Name",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "web_conference",
                    "label": "Web Conference",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : False,
                    "isDefault" : False
                },
                {
                    "domain": [

                    ],
                    "key": "buyer_interest_score",
                    "label": "Buyer iInterest Score",
                    "type": "slider",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "participants.talk_ratio",
                    "label": "Talk ratio",
                    "type": "slider",
                    "sub_filters": None,
                    "isActive" : False,
                    "isDefault" : False
                },
                {
                    "domain": [

                    ],
                    "key": "participants.interactivity_ratio",
                    "label": "Interactivity ratio",
                    "type": "slider",
                    "sub_filters": None,
                    "isActive" : False,
                    "isDefault" : False
                },
                {
                    "domain": [

                    ],
                    "key": "participants.longest_monologue",
                    "label": "Longest Monologue",
                    "type": "slider",
                    "sub_filters": None,
                    "isActive" : False,
                    "isDefault" : False
                }
            ]
        }
    },
    {
        "index": "Deals",
        "filter_fields":
        {
            'order': ['close_date', #date
                    'account', #object
                    'amount',  #slider
                    'owner_name',
                    'stage',
                    'engagement_grade',
                    'forecast_category',
                    'region',
                    'vertical',
                    'win_score'], #slider
            'schema': [
                {
                    "domain": [

                    ],
                    "key": "close_date",
                    "label": "Close Date",
                    "type": "date",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "account",
                    "label": "Account",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "amount",
                    "label": "Amount",
                    "type": "slider",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "owner_name",
                    "label": "Owner Name",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "stage",
                    "label": "Stage",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "engagement_grade",
                    "label": "Engagement Grade",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "forecast_category",
                    "label": "Forecast Category",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "region",
                    "label": "Region",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : False
                },
                {
                    "domain": [

                    ],
                    "key": "vertical",
                    "label": "Vertical",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : False
                },
                {
                    "domain": [

                    ],
                    "key": "win_score",
                    "label": "Win Score",
                    "type": "slider",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : False
                }
            ]
        }
    },
    {
        "index": "Accounts",
        "filter_fields":
        {
            'order': ['owner_name',
                    'vertical',
                    'engagement_grade'],
            'schema': [
                {
                    "domain": [

                    ],
                    "key": "owner_name",
                    "label": "Account Owner",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "vertical",
                    "label": "Vertical",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                },
                {
                    "domain": [

                    ],
                    "key": "engagement_grade",
                    "label": "Engangement grade",
                    "type": "object",
                    "sub_filters": None,
                    "isActive" : True,
                    "isDefault" : True
                }
            ]
        }
    }
]
