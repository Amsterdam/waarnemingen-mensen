"""
Django settings for waarnemingen-mensen project.

Generated by 'django-admin startproject' using Django 2.1.2.

For more information on this file, see
https://docs.djangoproject.com/en/2.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/2.1/ref/settings/
"""

import os
import sys

import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv('SECRET_KEY')
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'

# The token that is allowed to post data to protected endpoints
AUTHORIZATION_TOKEN = os.environ['AUTHORIZATION_TOKEN']
GET_AUTHORIZATION_TOKEN = os.environ['GET_AUTHORIZATION_TOKEN']

ALLOWED_HOSTS = ['*']
X_FRAME_OPTIONS = 'ALLOW-FROM *'
INTERNAL_IPS = ('127.0.0.1', '0.0.0.0')

# Application definition
HEALTH_MODEL = 'peoplemeasurement.PeopleMeasurement'

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]


INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "django_filters",
    "django_extensions",
    "django.contrib.gis",
    "datapunt_api",
    "rest_framework",
    "rest_framework_gis",
    'health',
    'datetimeutc',
    'settings',
    'peoplemeasurement',
    'telcameras_v2',
]

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

# Database
# https://docs.djangoproject.com/en/2.1/ref/settings/#databases

ROOT_URLCONF = "settings.urls"

WSGI_APPLICATION = "settings.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.contrib.gis.db.backends.postgis",
        "NAME": os.getenv("DATABASE_NAME", "waarnemingen_mensen"),
        "USER": os.getenv("DATABASE_USER", "waarnemingen_mensen"),
        "PASSWORD": os.getenv("DATABASE_PASSWORD", "insecure"),
        "HOST": os.getenv("DATABASE_HOST", "database"),
        "CONN_MAX_AGE": 20,
        "PORT": os.getenv("DATABASE_PORT", "5432"),
    },
}


TIME_ZONE = 'Europe/Amsterdam'
USE_TZ = True

LANGUAGE_CODE = 'en-us'
USE_I18N = True
USE_L10N = True

DUMP_DIR = "mks-dump"
TESTING = len(sys.argv) > 1 and sys.argv[1] == "test"


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.1/howto/static-files/

STATIC_URL = '/mensen/static/'
STATIC_ROOT = '/static/'

if DEBUG:
    INSTALLED_APPS += ('debug_toolbar',)
    MIDDLEWARE += (
        # 'corsheaders.middleware.CorsMiddleware',
        'debug_toolbar.middleware.DebugToolbarMiddleware',
    )
    CORS_ORIGIN_ALLOW_ALL = True
    DEBUG_TOOLBAR_PANELS = [
        'debug_toolbar.panels.versions.VersionsPanel',
        'debug_toolbar.panels.timer.TimerPanel',
        'debug_toolbar.panels.settings.SettingsPanel',
        'debug_toolbar.panels.headers.HeadersPanel',
        'debug_toolbar.panels.request.RequestPanel',
        'debug_toolbar.panels.sql.SQLPanel',
        'debug_toolbar.panels.staticfiles.StaticFilesPanel',
        'debug_toolbar.panels.templates.TemplatesPanel',
        'debug_toolbar.panels.cache.CachePanel',
        'debug_toolbar.panels.signals.SignalsPanel',
        'debug_toolbar.panels.logging.LoggingPanel',
        'debug_toolbar.panels.redirects.RedirectsPanel',
        'debug_toolbar.panels.profiling.ProfilingPanel',
    ]


REST_FRAMEWORK = dict(
    PAGE_SIZE=20,
    MAX_PAGINATE_BY=100,
    UNAUTHENTICATED_USER={},
    UNAUTHENTICATED_TOKEN={},
    DEFAULT_AUTHENTICATION_CLASSES=(
        'contrib.rest_framework.authentication.SimplePostTokenAuthentication',
    ),
    DEFAULT_PAGINATION_CLASS=("datapunt_api.pagination.HALPagination",),
    DEFAULT_RENDERER_CLASSES=(
        "rest_framework.renderers.JSONRenderer",
        "datapunt_api.renderers.PaginatedCSVRenderer",
        "rest_framework.renderers.BrowsableAPIRenderer",
        "rest_framework_xml.renderers.XMLRenderer",  # must be lowest!
    ),
    DEFAULT_FILTER_BACKENDS=(
        # 'rest_framework.filters.SearchFilter',
        # 'rest_framework.filters.OrderingFilter',
        "django_filters.rest_framework.DjangoFilterBackend"
    ),
    DEFAULT_VERSIONING_CLASS='rest_framework.versioning.NamespaceVersioning',
    COERCE_DECIMAL_TO_STRING=True,
)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "console": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "console"
        }
    },
    "root": {"level": "DEBUG", "handlers": ["console"]},
    "loggers": {
        "django.db": {
            "handlers": ["console"],
            "level": "ERROR"
        },
        "django": {
            "handlers": ["console"],
            "level": "ERROR"
        },
        # Debug all batch jobs
        "doc": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        },
        "index": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False
        },
        "search": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False
        },
        "elasticsearch": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False
        },
        "urllib3": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False
        },
        "factory.containers": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        },
        "factory.generate": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        },
        "requests.packages.urllib3.connectionpool": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False
        },
        # Log all unhandled exceptions
        "django.request": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False
        },
    },
}

SENTRY_DSN = os.getenv('SENTRY_DSN')
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[DjangoIntegration()],
        ignore_errors=['ExpiredSignatureError'],
        request_bodies='always'
    )
