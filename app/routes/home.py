import random
from flask import Blueprint, render_template, request, jsonify, session, Response, redirect, url_for
from app.services.task_manager import start_collection_task, get_task_status, stop_task
from app.services.cache_service import get_history
from app.translations import TRANSLATIONS
from core.row_config import rows_dict, DECLARANT_TYPES, TYPE_OPTIONS, INST_GROUPS, INSTITUTIONS, GROUP_TO_INST

home_bp = Blueprint("home", __name__)

# --- CAPTCHA CONFIG ---
ICONS = {
    "icon_car": "fa-car",
    "icon_tree": "fa-tree",
    "icon_user": "fa-user",
    "icon_home": "fa-home",
    "icon_bell": "fa-bell",
    "icon_camera": "fa-camera",
    "icon_plane": "fa-plane",
    "icon_star": "fa-star",
    "icon_heart": "fa-heart"
}


@home_bp.route("/set-lang/<lang_code>")
def set_language(lang_code):
    if lang_code in ['en', 'hy']:
        session['lang'] = lang_code
    # Redirect to referrer or default to landing page
    return redirect(request.referrer or url_for('home.index'))


@home_bp.route("/")
def index():
    """
    New Landing/Greeting Page.
    """
    return render_template("landing.html")


@home_bp.route("/tool")
def tool():
    """
    The main extraction tool page (formerly home).
    """
    context = {
        "rows_dict": rows_dict,
        "declarant_types": DECLARANT_TYPES,
        "type_options": TYPE_OPTIONS,
        "inst_groups": INST_GROUPS,
        "institutions": INSTITUTIONS,
        "group_to_inst": GROUP_TO_INST
    }
    return render_template("home.html", **context)


@home_bp.route("/captcha-puzzle")
def get_captcha_puzzle():
    """Generates a 3x3 grid puzzle for the frontend."""
    lang = session.get('lang', 'hy')

    # 1. Pick a target key (e.g., 'icon_car')
    keys = list(ICONS.keys())
    target_key = random.choice(keys)
    target_class = ICONS[target_key]

    # 2. Get target translation
    target_name = TRANSLATIONS.get(lang, {}).get(target_key, "Icon")

    # 3. Generate Grid (3x3 = 9 items)
    num_correct = random.randint(2, 4)
    grid_items = [target_class] * num_correct
    distractors = [v for k, v in ICONS.items() if k != target_key]

    while len(grid_items) < 9:
        grid_items.append(random.choice(distractors))

    random.shuffle(grid_items)

    correct_indices = [i for i, x in enumerate(grid_items) if x == target_class]
    session['captcha_solution'] = sorted(correct_indices)

    return jsonify({
        "target_name": target_name,
        "grid": grid_items
    })


@home_bp.route("/start", methods=["POST"])
def start_process():
    try:
        raw = request.json
        if not raw:
            return jsonify({"error": "No JSON data received"}), 400

        # --- ICON CAPTCHA CHECK ---
        user_selection = raw.get("captcha_selection", [])
        real_solution = session.get("captcha_solution")
        session.pop("captcha_solution", None)  # One-time use

        if not real_solution or not user_selection:
            return jsonify({"error": "CAPTCHA_FAIL"}), 400

        user_selection = sorted([int(x) for x in user_selection])

        if user_selection != real_solution:
            return jsonify({"error": "CAPTCHA_FAIL"}), 400

        # --------------------------

        def clean_int(val):
            if not val: return None
            try:
                i = int(val)
                return i if i > 0 else None
            except ValueError:
                return None

        def required_int(val):
            try:
                return int(val)
            except:
                return 0

        filters = {
            "row_name": raw.get("row_name"),
            "year": required_int(raw.get("year")),
            "declarant_type": clean_int(raw.get("declarant_type")),
            "t_type": clean_int(raw.get("t_type")),
            "inst_group": clean_int(raw.get("inst_group")),
            "institution": clean_int(raw.get("institution")),
            "limit": required_int(raw.get("limit") or 100),
            "offset": required_int(raw.get("offset")),
            "retry_ids": raw.get("retry_ids", [])
        }

        if not filters["row_name"] or not filters["year"]:
            return jsonify({"error": "MISSING_FIELDS"}), 400

        task_id = start_collection_task(filters)
        return jsonify({"task_id": task_id})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@home_bp.route("/history")
def history():
    items = get_history()
    return render_template("history.html", history_items=items)


@home_bp.route("/status/<task_id>")
def status(task_id):
    info = get_task_status(task_id)
    if not info:
        return jsonify({"status": "unknown"}), 404
    return jsonify(info)


@home_bp.route("/stop/<task_id>", methods=["POST"])
def stop_process(task_id):
    stopped = stop_task(task_id)
    return jsonify({"success": stopped})
