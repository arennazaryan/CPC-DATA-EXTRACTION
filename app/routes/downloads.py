from flask import Blueprint, send_file, flash, redirect, url_for
from app.services.cache_service import get_dataset_download_info

downloads_bp = Blueprint("downloads", __name__)

@downloads_bp.route("/download/<task_id>")
def download_csv(task_id):
    download_info = get_dataset_download_info(task_id)

    if not download_info or download_info.get("type") != "file":
        flash("File expired or deleted by the server reset.", "danger")
        return redirect(url_for("home.index"))

    return send_file(
        download_info["path"],
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"cpc_data_{task_id}.csv"
    )
