"""
outreach/ui.py
Streamlit review UI for outreach drafts.
Tabs: Queue | Generate | Sent | Replies | Stats | Test
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st
from storage.db import (
    connect, init_outreach_tables, get_qualified_leads,
    get_pending_drafts, log_outreach, mark_sent, mark_skipped, get_stats,
)
from generator import generate_email
from email_sender import pick_account, send_email, _load_accounts

st.set_page_config(page_title="Outreach", page_icon="📨", layout="wide")

# ── minimal style ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stMetricValue"] { font-size: 2rem; }
.stTabs [data-baseweb="tab"] { font-size: 0.9rem; padding: 0.4rem 1rem; }
.stExpander { border: 1px solid #2a2a2a; border-radius: 6px; margin-bottom: 0.5rem; }
textarea { font-family: monospace !important; font-size: 0.85rem !important; }
</style>
""", unsafe_allow_html=True)

st.title("📨 Outreach")

conn = connect()
init_outreach_tables(conn)

stats = get_stats(conn)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Sent", stats["sent"])
c2.metric("Pending", stats["pending"])
c3.metric("Replies", stats["replies"])
c4.metric("Failed", stats["failed"])
c5.metric("Skipped", stats["skipped"])

st.divider()

tab_queue, tab_generate, tab_sent, tab_replies, tab_stats, tab_test = st.tabs(
    ["Queue", "Generate", "Sent", "Replies", "Stats", "⚙ Test"]
)

# ── Queue ─────────────────────────────────────────────────────────────────────
with tab_queue:
    drafts = get_pending_drafts(conn)
    if not drafts:
        st.info("No pending drafts — head to Generate to create some.")
    else:
        st.caption(f"{len(drafts)} drafts waiting for review")

    for d in drafts:
        header = f"**{d['name']}** · {d['address']} · {d['created_at'][:10]}"
        with st.expander(header, expanded=False):
            subject = st.text_input("Subject", value=d["subject"] or "", key=f"subj_{d['id']}")
            body = st.text_area("Message", value=d["message"], height=180, key=f"body_{d['id']}")
            if d.get("outreach_angle"):
                st.caption(f"💡 Angle: {d['outreach_angle']}")
            col1, col2, col3 = st.columns([1, 1, 4])
            with col1:
                if st.button("Send", key=f"send_{d['id']}", type="primary"):
                    acc = pick_account(conn)
                    if acc is None:
                        st.error("All accounts at daily limit.")
                    else:
                        try:
                            send_email(d["address"], subject, body, acc)
                            conn.execute(
                                "UPDATE outreach_log SET message=?, subject=? WHERE id=?",
                                (body, subject, d["id"]),
                            )
                            conn.commit()
                            mark_sent(conn, d["id"])
                            st.success(f"✓ Sent via {acc['address']}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Send failed: {e}")
            with col2:
                if st.button("Skip", key=f"skip_{d['id']}"):
                    mark_skipped(conn, d["id"])
                    st.rerun()

# ── Generate ──────────────────────────────────────────────────────────────────
with tab_generate:
    leads = get_qualified_leads(conn)
    st.caption(f"{len(leads)} qualified leads without outreach")

    if leads:
        col_all, _ = st.columns([1, 3])
        with col_all:
            if st.button("Generate all drafts", type="primary"):
                progress = st.progress(0)
                errors = []
                for i, lead in enumerate(leads):
                    try:
                        lead = dict(lead)
                        result = generate_email(lead)
                        emails = (lead.get("site_emails") or lead.get("email_maps") or "").split(",")
                        to_addr = next((e.strip() for e in emails if "@" in e), "")
                        if not to_addr:
                            errors.append(f"{lead['name']}: no email")
                            continue
                        log_outreach(conn, lead["id"], "email", to_addr,
                                     result["body"], result["subject"], status="pending")
                    except Exception as e:
                        errors.append(f"{lead['name']}: {e}")
                    progress.progress((i + 1) / len(leads))
                st.success(f"Generated {len(leads) - len(errors)} drafts")
                if errors:
                    st.warning("\n".join(errors))
                st.rerun()

        st.divider()
        names = [f"{l['name']}  (id {l['id']})" for l in leads]
        choice = st.selectbox("Or generate one", names)
        idx = names.index(choice)
        lead = dict(leads[idx])

        if st.button("Generate"):
            with st.spinner("Generating..."):
                result = generate_email(lead)
            st.session_state["_gen_result"] = result
            st.session_state["_gen_lead"] = lead

        if "_gen_result" in st.session_state:
            result = st.session_state["_gen_result"]
            lead = st.session_state["_gen_lead"]
            subject = st.text_input("Subject", value=result["subject"], key="gen_subj")
            body = st.text_area("Body", value=result["body"], height=180, key="gen_body")
            emails = (lead.get("site_emails") or lead.get("email_maps") or "").split(",")
            to_addr = next((e.strip() for e in emails if "@" in e), "")
            st.caption(f"To: {to_addr}")
            if to_addr and st.button("Save as draft"):
                log_outreach(conn, lead["id"], "email", to_addr, body, subject)
                del st.session_state["_gen_result"]
                del st.session_state["_gen_lead"]
                st.success("Saved to queue")
                st.rerun()
    else:
        st.info("No qualified leads without outreach found.")

# ── Sent ──────────────────────────────────────────────────────────────────────
with tab_sent:
    rows = conn.execute(
        """
        SELECT o.id, b.name, o.channel, o.address, o.subject, o.sent_at
        FROM outreach_log o JOIN businesses b ON b.id = o.lead_id
        WHERE o.status = 'sent'
        ORDER BY o.sent_at DESC LIMIT 200
        """
    ).fetchall()
    if rows:
        import pandas as pd
        st.dataframe(
            pd.DataFrame([dict(r) for r in rows]),
            width='stretch',
            hide_index=True,
        )
    else:
        st.info("Nothing sent yet.")

# ── Replies ───────────────────────────────────────────────────────────────────
with tab_replies:
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Poll Gmail", type="primary"):
            from reply_listener import poll_replies
            with st.spinner("Polling inboxes..."):
                count = poll_replies()
            st.success(f"{count} new replies logged")
    with col2:
        if st.button("🤖 Classify replies"):
            from classifier import run_classifier
            with st.spinner("Classifying..."):
                count = run_classifier(conn)
            st.success(f"Classified {count} replies")

    st.divider()
    rows = conn.execute(
        """
        SELECT r.id, b.name, r.channel, r.received_at,
               rc.label, rc.confidence, r.content
        FROM replies r
        JOIN businesses b ON b.id = r.lead_id
        LEFT JOIN reply_classification rc ON rc.reply_id = r.id
        ORDER BY r.received_at DESC
        """
    ).fetchall()
    if not rows:
        st.info("No replies yet.")
    for r in rows:
        label = r["label"] or "unclassified"
        icon = {"interested": "🟢", "question": "🟡",
                "not_interested": "🔴", "ignore": "⚫"}.get(label, "⚪")
        conf = f" ({r['confidence']:.0%})" if r["confidence"] else ""
        with st.expander(f"{icon} {r['name']} — {label}{conf} · {(r['received_at'] or '')[:10]}"):
            st.write(r["content"])

# ── Stats ─────────────────────────────────────────────────────────────────────
with tab_stats:
    reply_breakdown = conn.execute(
        "SELECT label, COUNT(*) AS cnt FROM reply_classification GROUP BY label"
    ).fetchall()
    if reply_breakdown:
        import pandas as pd
        st.subheader("Reply breakdown")
        st.bar_chart(
            pd.DataFrame([dict(r) for r in reply_breakdown]).set_index("label")["cnt"]
        )

    daily = conn.execute(
        """
        SELECT DATE(sent_at) AS day, COUNT(*) AS sent
        FROM outreach_log WHERE status='sent'
        GROUP BY day ORDER BY day DESC LIMIT 30
        """
    ).fetchall()
    if daily:
        import pandas as pd
        st.subheader("Daily sends (last 30 days)")
        st.bar_chart(
            pd.DataFrame([dict(r) for r in daily]).set_index("day")["sent"]
        )

    account_breakdown = conn.execute(
        """
        SELECT address, COUNT(*) AS sent
        FROM outreach_log WHERE status='sent' AND channel='email'
        GROUP BY address
        """
    ).fetchall()
    if account_breakdown:
        import pandas as pd
        st.subheader("Sends per account")
        st.dataframe(
            pd.DataFrame([dict(r) for r in account_breakdown]),
            width='stretch', hide_index=True,
        )

# ── Test ──────────────────────────────────────────────────────────────────────
with tab_test:
    st.subheader("SMTP / IMAP test")
    st.caption("Send a test email between your configured accounts to verify credentials work.")

    accounts = _load_accounts()
    if not accounts:
        st.error("No email accounts found in .env")
    else:
        account_labels = [f"{a['name']} <{a['address']}>" for a in accounts]

        col1, col2 = st.columns(2)
        with col1:
            from_idx = st.selectbox("From", range(len(accounts)),
                                    format_func=lambda i: account_labels[i], key="test_from")
        with col2:
            to_idx = st.selectbox("To", range(len(accounts)),
                                  format_func=lambda i: account_labels[i], key="test_to")

        test_subject = st.text_input("Subject", value="SMTP test — biz outreach", key="test_subj")
        test_body = st.text_area("Body", value="This is a test email from the outreach pipeline.\n\nIf you're reading this, SMTP is working.", height=100, key="test_body")

        if st.button("Send test email", type="primary"):
            sender = accounts[from_idx]
            recipient = accounts[to_idx]["address"]
            with st.spinner(f"Sending via {sender['address']}..."):
                try:
                    send_email(recipient, test_subject, test_body, sender)
                    st.success(f"✓ Sent from {sender['address']} → {recipient}")
                    st.info("Check the inbox to confirm delivery. If it lands in spam, that's expected before warmup.")
                except Exception as e:
                    st.error(f"Failed: {e}")
                    st.warning("Common causes: app password wrong, 2FA not enabled, SMTP blocked by Google. Check https://myaccount.google.com/apppasswords")

        st.divider()
        st.subheader("Step-by-step SMTP diagnostic")
        st.caption("Runs each SMTP step separately so you can see exactly where it breaks.")
        if st.button("Run diagnostic", key="diag"):
            import smtplib
            sender = accounts[from_idx]
            pw = sender["password"]
            addr = sender["address"]
            steps = []

            # Step 1: TCP connect
            try:
                import socket
                s = socket.create_connection(("smtp.gmail.com", 587), timeout=10)
                s.close()
                steps.append(("✅ TCP connect to smtp.gmail.com:587", None))
            except Exception as e:
                steps.append(("❌ TCP connect failed", str(e)))

            # Step 2: EHLO + STARTTLS
            srv = None
            try:
                srv = smtplib.SMTP("smtp.gmail.com", 587, timeout=15)
                srv.ehlo()
                srv.starttls()
                srv.ehlo()
                steps.append(("✅ EHLO + STARTTLS", None))
            except Exception as e:
                steps.append(("❌ STARTTLS failed", str(e)))

            # Step 3: AUTH LOGIN
            if srv:
                try:
                    srv.login(addr, pw)
                    steps.append(("✅ AUTH LOGIN — credentials accepted", None))
                    srv.quit()
                except smtplib.SMTPAuthenticationError as e:
                    steps.append(("❌ AUTH LOGIN failed — bad credentials", str(e)))
                    st.session_state["_diag_auth_failed"] = True
                except Exception as e:
                    steps.append(("❌ AUTH LOGIN failed", str(e)))

            for label, detail in steps:
                if detail:
                    st.error(f"{label}\n\n`{detail}`")
                else:
                    st.success(label)

            if st.session_state.get("_diag_auth_failed"):
                st.warning(f"""
**App password checklist for `{addr}`:**
1. Go to [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
2. Make sure 2-Step Verification is ON for this account
3. Delete the old app password, create a new one (select "Mail" + "Mac")
4. Copy the 16-char password exactly — no spaces needed, we strip them automatically
5. Paste into `.env` as `EMAIL_X_PASSWORD=xxxx xxxx xxxx xxxx`
""")


        st.divider()
        st.subheader("Account status")
        rows = []
        for acc in accounts:
            sent_today = conn.execute(
                "SELECT COUNT(*) AS cnt FROM outreach_log WHERE channel='email' AND address=? AND status='sent' AND DATE(sent_at)=DATE('now')",
                (acc["address"],),
            ).fetchone()["cnt"]
            rows.append({
                "account": acc["address"],
                "name": acc["name"],
                "daily_limit": acc["daily_limit"],
                "sent_today": sent_today,
                "remaining": acc["daily_limit"] - sent_today,
            })
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)