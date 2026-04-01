"""
Structured high-ticket offer catalog for outreach and operator reference.

This is the source of truth for:
- niche -> offer mapping
- price bands
- plain-language pain statements
- deliverables
- GDPR-safe positioning
"""

OFFER_CATALOG = {
    "real_estate": {
        "primary_offer": "lead_qualification_followup_engine",
        "offers": {
            "lead_qualification_followup_engine": {
                "label": "AI Lead Qualification & Follow-Up Engine",
                "positioning": "Handles inbound buyer enquiries, qualification, and follow-up so agents stay focused on viewings and closings.",
                "pain": "Agents lose deals to slow replies and inconsistent follow-up.",
                "deliverables": [
                    "Inbound enquiry intake from email/forms/portals",
                    "Lead scoring by budget, location, and timeline",
                    "Drafted first replies and follow-up messages",
                    "Hot-lead alerts for the agent",
                    "Simple activity visibility across the enquiry flow",
                ],
                "setup_fee_eur": "697-1297 (intro) / 1297-2000 (standard)",
                "monthly_fee_eur": "397-597 (intro) / 597-900 (standard)",
                "gdpr_positioning": "All automation runs inside your systems. No personal data is stored externally. AI drafts and routes; your team keeps control.",
                "cta_variants": [
                    "Want a short outline of how that would look for your agency?",
                    "Useful for your team, yes or no?",
                    "Would that be worth showing in a short outline?",
                    "Relevant for your agency, yes or no?",
                ],
            },
            "listing_hunter_engine": {
                "label": "AI Listing Hunter",
                "positioning": "Finds new listing opportunities and drafts outbound outreach before competitors do.",
                "pain": "Agencies need new listings and waste time prospecting manually.",
                "deliverables": [
                    "Listing-source monitoring",
                    "New-property detection",
                    "Owner or agency contact extraction",
                    "Drafted outreach and follow-up",
                    "Warm-opportunity handoff to the agent",
                ],
                "setup_fee_eur": "3000-5000",
                "monthly_fee_eur": "500-1200",
                "gdpr_positioning": "Uses publicly available business data and keeps processing within your workflow. No private data profiling.",
                "cta_variants": [
                    "If I sent a short outline for that, would you look at it?",
                    "Worth a short outline, yes or no?",
                    "Would that be interesting for your team, yes or no?",
                    "Relevant enough to send a short outline?",
                ],
            },
        },
    },
    "home_services": {
        "primary_offer": "job_filter_quote_engine",
        "offers": {
            "job_filter_quote_engine": {
                "label": "AI Job Filter + Quote Drafting System",
                "positioning": "Filters inbound job requests, drafts replies or quotes, and books the good jobs faster.",
                "pain": "Trades waste time on weak leads and manual quote replies.",
                "deliverables": [
                    "Inbound lead parsing",
                    "Job-fit filtering",
                    "Quote or reply drafting",
                    "Follow-up on missed leads",
                    "Calendar booking for qualified jobs",
                ],
                "setup_fee_eur": "497-697 (intro) / 697-997 (standard)",
                "monthly_fee_eur": "197-397 (intro) / 397-597 (standard)",
                "gdpr_positioning": "Processes only the messages you already receive inside your existing workflow.",
                "cta_variants": [
                    "Would that save your team time, yes or no?",
                    "Useful if it cut the admin around new jobs, yes or no?",
                    "Want a short outline of that?",
                    "Relevant for your business, yes or no?",
                ],
            },
            "review_followup_engine": {
                "label": "AI Review Booster + Customer Follow-Up Engine",
                "positioning": "Triggers review asks, handles post-job follow-up, and helps book repeat work.",
                "pain": "Trades leave reviews and repeat jobs on the table because nobody follows up consistently.",
                "deliverables": [
                    "Completed-job follow-up triggers",
                    "Review request flow",
                    "Complaint-routing drafts",
                    "Repeat-work reminders",
                    "Simple customer follow-up tracking",
                ],
                "setup_fee_eur": "1500-3000",
                "monthly_fee_eur": "300-800",
                "gdpr_positioning": "Runs from your existing channels. Customer data stays within your systems.",
                "cta_variants": [
                    "Useful if it brought in more reviews and repeat work, yes or no?",
                    "Would that be worth a short outline?",
                    "Relevant for your team, yes or no?",
                    "Want me to send a short outline?",
                ],
            },
        },
    },
    "accounting_tax": {
        "primary_offer": "document_intake_categorization_system",
        "offers": {
            "document_intake_categorization_system": {
                "label": "AI Document Intake & Categorization System",
                "positioning": "Sorts inbound documents, flags missing items, and reduces manual document chasing.",
                "pain": "Accounting teams lose hours to repetitive document intake and follow-up.",
                "deliverables": [
                    "Inbound document reading and categorization",
                    "Missing-document detection",
                    "Drafted client reminders",
                    "Structured handoff into the firm's workflow",
                    "Simple visibility on outstanding items",
                ],
                "setup_fee_eur": "797-1497 (intro) / 1497-2500 (standard)",
                "monthly_fee_eur": "497-797 (intro) / 797-1200 (standard)",
                "gdpr_positioning": "Processing stays inside your environment. No client data is used for model training.",
                "cta_variants": [
                    "Useful if it cut document chasing, yes or no?",
                    "Would that be worth a short outline?",
                    "Relevant for your firm, yes or no?",
                    "Want me to send a short outline?",
                ],
            },
            "client_deadline_manager": {
                "label": "AI Client Communication & Deadline Manager",
                "positioning": "Tracks missing items and deadlines, drafts reminders, and escalates only when needed.",
                "pain": "Deadlines get risky when clients send documents late and the team has to chase manually.",
                "deliverables": [
                    "Deadline tracking",
                    "Reminder and follow-up drafting",
                    "Escalation rules for overdue items",
                    "Operator approval before send",
                    "Visibility on at-risk client workflows",
                ],
                "setup_fee_eur": "3000-5000 (standard)",
                "monthly_fee_eur": "600-1200 (standard)",
                "gdpr_positioning": "AI drafts messages; your team stays in control. No external storage or model training on client data.",
                "cta_variants": [
                    "Useful if it reduced deadline chasing, yes or no?",
                    "Would that be relevant for your firm, yes or no?",
                    "Want a short outline of that?",
                    "Worth showing in a short outline?",
                ],
            },
        },
    },
}

