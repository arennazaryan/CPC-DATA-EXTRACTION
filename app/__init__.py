from flask import Flask, session
from flask_wtf.csrf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix
from app.config import Config
from app.translations import TRANSLATIONS

csrf = CSRFProtect()
limiter = Limiter(key_func=get_remote_address)

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(Config)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    csrf.init_app(app)
    limiter.init_app(app)

    @app.context_processor
    def inject_content():
        lang = session.get('lang', 'hy')
        def t(key):
            return TRANSLATIONS.get(lang, {}).get(key, key)
        return dict(t=t, get_lang=lambda: lang)

    from app.routes.home import home_bp
    from app.routes.results import results_bp
    from app.routes.downloads import downloads_bp

    app.register_blueprint(home_bp)
    app.register_blueprint(results_bp)
    app.register_blueprint(downloads_bp)

    return app
