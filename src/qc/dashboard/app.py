# src/qc/dashboard/app.py
from flask import Flask, render_template, request, url_for, redirect, flash
from markupsafe import Markup
from qc.dashboard.loader import (
    load_latest_summaries, load_issues, load_delta_report, load_trends,
    list_suites, load_suite_config, save_suite_config,
)

app = Flask(__name__)
app.secret_key = "qc-dashboard-dev"


@app.template_filter("format_metric")
def format_metric(value, is_rate: bool) -> str:
    if value is None:
        return "—"
    if is_rate:
        return f"{value * 100:.2f}%"
    if isinstance(value, float):
        return f"{value:,.3f}"
    return f"{value:,}"


@app.template_filter("format_delta")
def format_delta(value, is_rate: bool) -> Markup:
    if value is None:
        return Markup("—")
    sign = "+" if value > 0 else ""
    if is_rate:
        color = "#c0392b" if value > 0 else "#1a7a40"
        return Markup(f'<span style="color:{color};font-weight:bold">{sign}{value * 100:.2f}</span>')
    color = "#c0392b" if abs(value) > 10 else "#e67e22" if abs(value) > 5 else "#1a7a40"
    return Markup(f'<span style="color:{color};font-weight:bold">{sign}{value:.1f}</span>')


@app.route("/")
def summary():
    rows = load_latest_summaries()
    return render_template("summary.html", rows=rows)


@app.route("/issues")
def issues():
    import pandas as pd
    client = request.args.get("client") or None
    domain = request.args.get("domain") or None
    run_date = request.args.get("run_date") or None
    rule = request.args.get("rule") or None
    severity = request.args.get("severity") or None
    try:
        page = max(1, int(request.args.get("page", 1)))
    except (ValueError, TypeError):
        page = 1

    df, _ = load_issues(client, domain, run_date, rule, severity, page=1, page_size=10000)

    # Merge temporal (delta) flags into the same table
    if client and domain and run_date:
        delta = load_delta_report(client, domain, run_date)
        delta_flags = delta.get("flags", [])
        if delta_flags:
            delta_df = pd.DataFrame(delta_flags)
            for col in ["row_id", "column", "detail"]:
                if col not in delta_df.columns:
                    delta_df[col] = None
            if not df.empty:
                df = pd.concat([df, delta_df], ignore_index=True)
            else:
                df = delta_df

    # Apply rule/severity filters across the combined set
    if rule and not df.empty:
        df = df[df["rule"] == rule]
    if severity and not df.empty:
        df = df[df["severity"] == severity]

    total = len(df)
    page_size = 100
    start = (page - 1) * page_size
    page_df = df.iloc[start:start + page_size] if not df.empty else df

    filter_params = {k: v for k, v in {
        "client": client, "domain": domain, "run_date": run_date,
        "rule": rule, "severity": severity
    }.items() if v}

    prev_url = url_for("issues", **filter_params, page=page - 1) if page > 1 else None
    next_url = url_for("issues", **filter_params, page=page + 1) if page * page_size < total else None

    return render_template(
        "issues.html",
        rows=page_df.to_dict("records") if not page_df.empty else [],
        total=total,
        page=page,
        page_size=page_size,
        filters={"client": client, "domain": domain, "run_date": run_date,
                 "rule": rule, "severity": severity},
        prev_url=prev_url,
        next_url=next_url,
    )


@app.route("/trends")
def trends():
    client = request.args.get("client") or None
    domain = request.args.get("domain") or None
    run_date = request.args.get("run_date") or None
    rows = []
    if client and domain and run_date:
        rows = load_trends(client, domain, run_date)
    # Populate selector options from latest summaries
    summaries = load_latest_summaries()
    return render_template(
        "trends.html",
        rows=rows,
        filters={"client": client, "domain": domain, "run_date": run_date},
        summaries=summaries,
    )


_DEFAULT_SUITE_YAML = """\
client: {client}
domain: {domain}
checks:
  - missing:
      columns: []
      max_null_rate: 0.0
  - logic:
      lwbs:
        description: "Left Without Being Seen"
        condition: "disposition == 'LWBS'"
        max_rate: 0.05
        severity: warn
  - delta:
      row_count: {{max_pct_change: 0.30}}
      null_rate: {{max_abs_change: 0.05}}
"""


@app.route("/config/validate", methods=["POST"])
def config_validate():
    import yaml as _yaml
    from flask import jsonify
    yaml_text = request.get_data(as_text=True)
    try:
        _yaml.safe_load(yaml_text)
        return jsonify({"valid": True})
    except _yaml.YAMLError as exc:
        return jsonify({"valid": False, "error": str(exc)})


@app.route("/config", methods=["GET"])
def config_view():
    client = request.args.get("client") or None
    domain = request.args.get("domain") or None
    suites = list_suites()
    yaml_text = None
    if client and domain:
        existing = load_suite_config(client, domain)
        yaml_text = existing if existing is not None else _DEFAULT_SUITE_YAML.format(
            client=client, domain=domain
        )
    return render_template(
        "config.html",
        suites=suites,
        filters={"client": client, "domain": domain},
        yaml_text=yaml_text,
        default_yaml=_DEFAULT_SUITE_YAML.format(client="", domain=""),
    )


@app.route("/config", methods=["POST"])
def config_save():
    client = request.form.get("client", "").strip()
    domain = request.form.get("domain", "").strip()
    yaml_text = request.form.get("yaml_text", "")
    if not client or not domain:
        flash("Client and domain are required.", "error")
        return redirect(url_for("config_view"))
    error = save_suite_config(client, domain, yaml_text)
    if error:
        flash(error, "error")
    else:
        flash(f"Saved {client}/{domain}.yaml", "success")
    return redirect(url_for("config_view", client=client, domain=domain))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
