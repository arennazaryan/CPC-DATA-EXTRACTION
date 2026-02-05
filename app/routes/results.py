from flask import Blueprint, render_template, redirect, url_for, flash
# from app.auth import login_required  <-- REMOVED
from app.services.cache_service import load_dataset

results_bp = Blueprint("results", __name__)

@results_bp.route("/results/<task_id>")
# @login_required <-- REMOVED
def show_results(task_id):
    # Load data from the cache (CSV/JSON files)
    df, metadata = load_dataset(task_id)

    if df is None:
        flash("Result file not found or expired.", "danger")
        return redirect(url_for("home.home"))

    # FIX: Handle empty data frame safely
    if df.empty:
        total_rows = 0
        preview_data = []
        columns = []
    else:
        total_rows = len(df)
        preview_data = df.head(50).to_dict(orient="records")
        columns = df.columns.tolist()

    return render_template(
        "results.html",
        task_id=task_id,
        preview_data=preview_data,
        columns=columns,
        metadata=metadata,
        total_rows=total_rows,
        failed_ids=metadata.get("failed_ids", [])
    )