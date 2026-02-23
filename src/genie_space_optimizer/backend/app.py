from .core import create_app
from .router import router

# Import route modules so their @router decorators register on the singleton.
# The singleton router (from create_router()) collects all routes under /api/genie.
from .routes import spaces as _spaces  # noqa: F401
from .routes import runs as _runs  # noqa: F401
from .routes import activity as _activity  # noqa: F401

app = create_app(routers=[router])
