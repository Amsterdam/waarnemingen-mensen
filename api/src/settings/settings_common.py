"""
Common django settings.
"""

import os
import sys

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# SECURITY WARNING: keep the secret key used in production secret!
insecure_key = "insecure"
SECRET_KEY = os.getenv("SECRET_KEY", insecure_key)

DEBUG = SECRET_KEY == insecure_key

ALLOWED_HOSTS = ["*"]

DATAPUNT_API_URL = os.getenv(
    "DATAPUNT_API_URL", "https://api.data.amsterdam.nl/")


# Application definition
INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "django_filters",
    "django_extensions",
    "django.contrib.gis",
    "datapunt_api",
    "rest_framework",
    "rest_framework_gis",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

X_FRAME_OPTIONS = 'ALLOW-FROM *'

INTERNAL_IPS = ('127.0.0.1', '0.0.0.0')


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


TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.messages.context_processors.messages",
            ]
        },
    }
]

# Internationalization
# https://docs.djangoproject.com/en/1.9/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_L10N = True

USE_TZ = True

DUMP_DIR = "mks-dump"

TESTING = len(sys.argv) > 1 and sys.argv[1] == "test"


REST_FRAMEWORK = dict(
    PAGE_SIZE=20,
    MAX_PAGINATE_BY=100,
    UNAUTHENTICATED_USER={},
    UNAUTHENTICATED_TOKEN={},
    DEFAULT_AUTHENTICATION_CLASSES=(
        # 'rest_framework.authentication.BasicAuthentication',
        # 'rest_framework.authentication.SessionAuthentication',
    ),
    DEFAULT_PAGINATION_CLASS=("datapunt_api.pagination.HALPagination",),
    DEFAULT_RENDERER_CLASSES=(
        "rest_framework.renderers.JSONRenderer",
        "datapunt_api.renderers.PaginatedCSVRenderer",
        "rest_framework.renderers.BrowsableAPIRenderer",
        # must be lowest!
        "rest_framework_xml.renderers.XMLRenderer",
    ),
    DEFAULT_FILTER_BACKENDS=(
        # 'rest_framework.filters.SearchFilter',
        # 'rest_framework.filters.OrderingFilter',
        "django_filters.rest_framework.DjangoFilterBackend"
    ),
    DEFAULT_VERSIONING_CLASS='rest_framework.versioning.NamespaceVersioning',
    COERCE_DECIMAL_TO_STRING=True,
)

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.10/howto/static-files/

STATIC_URL = "/static/"

STATIC_ROOT = os.path.abspath(os.path.join(BASE_DIR, "../../../", "static"))

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