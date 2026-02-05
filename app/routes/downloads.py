from flask import Blueprint, send_file, flash, redirect, url_for
from app.services.cache_service import get_dataset_path

downloads_bp = Blueprint("downloads", __name__)


@downloads_bp.route("/download/<task_id>")
def download_csv(task_id):
    # Use the new function from cache_service
    path = get_dataset_path(task_id)

    if not path:
        flash("File expired or deleted.", "danger")
        return redirect(url_for("home.home"))

    # Force download the file
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"cpc_data_{task_id}.csv"
    )