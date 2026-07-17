# src/qc/dashboard/app.py
from flask import Flask, render_template, request, url_for
from qc.dashboard.loader import load_latest_summaries, load_issues, load_delta_report

app = Flask(__name__)


@app.route("/")
def summary():
    rows = load_latest_summaries()
    return render_template("summary.html", rows=rows)


@app.route("/issues")
def issues():
    client = request.args.get("client") or None
    domain = request.args.get("domain") or None
    run_date = request.args.get("run_date") or None
    rule = request.args.get("rule") or None
    severity = request.args.get("severity") or None
    page = int(request.args.get("page", 1))

    df, total = load_issues(client, domain, run_date, rule, severity, page)
    delta = {}
    if client and domain and run_date:
        delta = load_delta_report(client, domain, run_date)

    # Build stable filter params dict for pagination URLs
    filter_params = {k: v for k, v in {
        "client": client, "domain": domain, "run_date": run_date,
        "rule": rule, "severity": severity
    }.items() if v}

    prev_url = url_for("issues", **filter_params, page=page - 1) if page > 1 else None
    next_url = url_for("issues", **filter_params, page=page + 1) if page * 100 < total else None

    return render_template(
        "issues.html",
        rows=df.to_dict("records") if not df.empty else [],
        total=total,
        page=page,
        page_size=100,
        delta=delta,
        filters={"client": client, "domain": domain, "run_date": run_date,
                 "rule": rule, "severity": severity},
        prev_url=prev_url,
        next_url=next_url,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
