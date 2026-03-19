"""
outreach/generator.py
Deterministic outreach draft generator with language-aware variations.
"""
from __future__ import annotations

import hashlib
import re


DEFAULT_SUBJECTS = {
    "no_booking": "Quick question about bookings",
    "no_lead_capture": "Quick question about your website",
    "no_tracking": "Quick question about enquiries",
    "no_ecommerce": "Quick question about online orders",
    "no_client_portal": "Quick question about intake",
    "no_social": "Quick question about visibility",
    "no_website": "Quick question",
}

LANG_ALIASES = {
    "slovak": "sk",
    "slovencina": "sk",
    "slovenčina": "sk",
    "sk": "sk",
    "cs": "cs",
    "czech": "cs",
    "čeština": "cs",
    "cesky": "cs",
    "česky": "cs",
    "de": "de",
    "deutsch": "de",
    "german": "de",
    "at": "de",
    "austrian german": "de",
    "en": "en",
    "english": "en",
}

LANG_PACKS = {
    "en": {
        "greeting_named": "Hi {name},",
        "greeting_plain": "Hi,",
        "greeting_options": ["Hi,", "Hello,", "Hey,", "Good morning,"],
        "close": [
            "Happy to keep this brief.",
            "No need for a long reply.",
            "Happy to keep it short.",
            "A quick reply would be plenty.",
        ],
        "soft_close": [
            "Let me know.",
            "Curious either way.",
            "Would be good to know.",
            "Open to a quick reply.",
        ],
        "fallback_subject": "Quick question",
        "fallback_body": [
            "I had a quick question about how enquiries are handled on your side.",
            "I wanted to ask how new enquiries are usually handled internally.",
            "I was curious how incoming enquiries are being handled at the moment.",
            "I had a quick question about how follow-up is managed after someone reaches out.",
        ],
        "fallback_question": [
            "Is that already running smoothly, or does it still take a fair bit of manual work?",
            "Do you already have a setup you are happy with, or is it still a bit manual?",
            "Is that process already in a good place, or is it still something the team has to chase manually?",
            "Would you say that is working well already, or does it still create admin overhead?",
        ],
    },
    "sk": {
        "greeting_named": "Dobrý deň {name},",
        "greeting_plain": "Dobrý deň,",
        "greeting_options": ["Dobrý deň,", "Dobrý deň prajem,", "Ahojte,", "Zdravím,"],
        "close": [
            "Stačí aj stručná odpoveď.",
            "Kľudne len krátko.",
            "Netreba dlhú odpoveď.",
            "Pokojne aj jednou vetou.",
        ],
        "soft_close": [
            "Dajte vedieť.",
            "Zaujíma ma to aj stručne.",
            "Pokojne len krátko.",
            "Stačí aj krátka odpoveď.",
        ],
        "fallback_subject": "Krátka otázka",
        "fallback_body": [
            "Chcel som sa krátko opýtať, ako u vás funguje spracovanie nových dopytov.",
            "Mal som krátku otázku k tomu, ako u vás riešite nové dopyty.",
            "Zaujímalo ma, ako máte momentálne nastavené spracovanie nových kontaktov.",
            "Chcel som sa opýtať, ako u vás prebieha nadväzujúca komunikácia po prvom kontakte.",
        ],
        "fallback_question": [
            "Funguje to už hladko, alebo je v tom stále dosť ručnej práce?",
            "Máte to už vyriešené, alebo to ešte dosť zaťažuje recepciu?",
            "Je to u vás už dobre nastavené, alebo je tam stále veľa manuálneho dohľadávania?",
            "Ide to už bez problémov, alebo to ešte vytvára zbytočný administratívny tlak?",
        ],
    },
    "cs": {
        "greeting_named": "Dobrý den {name},",
        "greeting_plain": "Dobrý den,",
        "greeting_options": ["Dobrý den,", "Dobrý den přeji,", "Ahoj,", "Zdravím,"],
        "close": [
            "Stačí i stručná odpověď.",
            "Klidně jen krátce.",
            "Není potřeba dlouhá odpověď.",
            "Klidně i jednou větou.",
        ],
        "soft_close": [
            "Dejte vědět.",
            "Zajímá mě to i stručně.",
            "Klidně jen krátce.",
            "Stačí i krátká odpověď.",
        ],
        "fallback_subject": "Krátký dotaz",
        "fallback_body": [
            "Chtěl jsem se krátce zeptat, jak u vás funguje práce s novými poptávkami.",
            "Měl jsem krátký dotaz k tomu, jak u vás řešíte nové poptávky.",
            "Zajímalo mě, jak máte teď nastavené zpracování nových kontaktů.",
            "Chtěl jsem se zeptat, jak u vás probíhá navazující komunikace po prvním kontaktu.",
        ],
        "fallback_question": [
            "Funguje to už hladce, nebo je v tom pořád dost ruční práce?",
            "Máte to už vyřešené, nebo to stále dost zatěžuje recepci?",
            "Je to už dobře nastavené, nebo je kolem toho pořád hodně manuální práce?",
            "Šlape to už bez problémů, nebo to pořád vytváří zbytečnou administrativu?",
        ],
    },
    "de": {
        "greeting_named": "Guten Tag {name},",
        "greeting_plain": "Guten Tag,",
        "greeting_options": ["Guten Tag,", "Hallo,", "Servus,", "Grüße Sie,"],
        "close": [
            "Eine kurze Antwort reicht völlig.",
            "Gern auch nur ganz kurz.",
            "Es braucht keine lange Antwort.",
            "Eine knappe Rückmeldung wäre schon hilfreich.",
        ],
        "soft_close": [
            "Geben Sie gern kurz Bescheid.",
            "Mich würde das kurz interessieren.",
            "Eine kurze Rückmeldung reicht.",
            "Gern auch nur knapp.",
        ],
        "fallback_subject": "Kurze Frage",
        "fallback_body": [
            "Ich wollte kurz fragen, wie neue Anfragen bei Ihnen intern bearbeitet werden.",
            "Ich hatte eine kurze Frage dazu, wie Sie neue Anfragen aktuell handhaben.",
            "Mich würde interessieren, wie eingehende Anfragen momentan bei Ihnen weiterbearbeitet werden.",
            "Ich wollte kurz nachfragen, wie das Follow-up nach einer ersten Anfrage bei Ihnen läuft.",
        ],
        "fallback_question": [
            "Läuft das bereits rund, oder steckt da noch einiges an manueller Arbeit drin?",
            "Haben Sie dafür schon einen guten Ablauf, oder kostet das intern noch viel Zeit?",
            "Ist der Prozess bei Ihnen schon sauber gelöst, oder entsteht dabei noch unnötiger Aufwand?",
            "Funktioniert das bereits gut, oder braucht es intern noch viel manuelle Nacharbeit?",
        ],
    },
}

GENERIC_FRAMEWORKS = {
    "en": {
        "soft_offer_closes": [
            "If useful, I can send over a quick idea.",
            "Happy to send a short example if relevant.",
            "Can send a quick outline if that's useful.",
            "I can share a simple approach if helpful.",
        ],
    },
    "sk": {
        "soft_offer_closes": [
            "Ak dáva zmysel, môžem poslať krátky nápad.",
            "Kľudne pošlem stručný príklad, ak to je relevantné.",
            "Ak chcete, viem poslať krátky návrh.",
            "Môžem poslať jednoduchý postup, ak by to pomohlo.",
        ],
    },
}

OPPORTUNITY_PACKS = {
    "high_value_case_followup": {
        "en": {
            "subjects": [
                "Quick question about implant enquiries",
                "Quick question about treatment follow-up",
                "Quick question about new patient enquiries",
                "Quick question about consultation enquiries",
            ],
            "observations": [
                "I noticed treatments like implants and esthetic work seem to be an important part of the clinic.",
                "It looks like higher-value treatments are a meaningful part of what the clinic offers.",
                "I saw that implants and esthetic cases seem to play a visible role in the practice.",
                "It seems the clinic handles treatments where timely follow-up really matters.",
            ],
            "questions": [
                "When someone enquires about that kind of treatment, is the follow-up mostly handled manually?",
                "After a patient asks about those treatments, is keeping the conversation warm still mainly a manual process?",
                "Once somebody reaches out about that kind of treatment, do you already have a good follow-up flow in place?",
                "When those enquiries come in, is the follow-up already structured well, or does it still depend on manual chasing?",
            ],
        },
        "sk": {
            "subjects": [
                "Krátka otázka k dopytom na implantáty",
                "Krátka otázka k follow-upu po dopyte",
                "Krátka otázka k novým pacientskym dopytom",
                "Krátka otázka ku konzultačným dopytom",
            ],
            "observations": [
                "Všimol som si, že implantáty a estetické zákroky tvoria viditeľnú časť ponuky kliniky.",
                "Vyzerá to, že hodnotnejšie zákroky sú u vás dôležitou časťou dopytov.",
                "Na webe je vidieť, že implantologické a estetické ošetrenia sú pre kliniku podstatné.",
                "Pôsobí to tak, že pri niektorých zákrokoch je rýchly follow-up naozaj dôležitý.",
            ],
            "questions": [
                "Keď príde dopyt na takýto zákrok, rieši sa follow-up stále hlavne ručne?",
                "Keď sa niekto ozve kvôli takémuto ošetreniu, drží to recepcia ešte manuálne?",
                "Po prvom dopyte už na to máte dobrý proces, alebo to ešte závisí od ručného dohľadávania?",
                "Keď takéto dopyty prídu, máte follow-up nastavený, alebo je v tom stále dosť manuálnej práce?",
            ],
        },
        "cs": {
            "subjects": [
                "Krátký dotaz k poptávkám na implantáty",
                "Krátký dotaz k follow-upu po poptávce",
                "Krátký dotaz k novým pacientským poptávkám",
                "Krátký dotaz ke konzultačním poptávkám",
            ],
            "observations": [
                "Všiml jsem si, že implantáty a estetické zákroky tvoří viditelnou část nabídky kliniky.",
                "Vypadá to, že hodnotnější zákroky jsou u vás důležitou částí poptávek.",
                "Na webu je vidět, že implantologické a estetické ošetření je pro kliniku podstatné.",
                "Působí to tak, že u některých zákroků opravdu záleží na rychlém follow-upu.",
            ],
            "questions": [
                "Když přijde poptávka na takový zákrok, řeší se follow-up stále hlavně ručně?",
                "Když se někdo ozve kvůli takovému ošetření, drží to recepce ještě manuálně?",
                "Máte už po prvním kontaktu dobrý proces, nebo to stále závisí na ručním dohledávání?",
                "Když takové poptávky přijdou, je follow-up už dobře nastavený, nebo je v tom pořád dost ruční práce?",
            ],
        },
        "de": {
            "subjects": [
                "Kurze Frage zu Implantat-Anfragen",
                "Kurze Frage zum Follow-up bei Behandlungsanfragen",
                "Kurze Frage zu neuen Patientenanfragen",
                "Kurze Frage zu Beratungsanfragen",
            ],
            "observations": [
                "Mir ist aufgefallen, dass Implantate und ästhetische Behandlungen ein sichtbarer Teil Ihres Angebots sind.",
                "Es wirkt so, als ob höherwertige Behandlungen bei Ihnen eine wichtige Rolle spielen.",
                "Auf der Website sieht man, dass implantologische und ästhetische Fälle für die Praxis relevant sind.",
                "Bei manchen Behandlungen scheint ein zügiges Follow-up besonders wichtig zu sein.",
            ],
            "questions": [
                "Wenn dazu eine Anfrage reinkommt, läuft das Follow-up noch überwiegend manuell?",
                "Wenn sich jemand zu so einer Behandlung meldet, wird das intern noch hauptsächlich händisch nachverfolgt?",
                "Haben Sie dafür nach der ersten Anfrage schon einen guten Ablauf, oder hängt es noch stark an manueller Nacharbeit?",
                "Wenn solche Anfragen reinkommen, ist der Follow-up-Prozess bereits sauber gelöst, oder braucht es intern noch viel Handarbeit?",
            ],
        },
    },
    "emergency_intake": {
        "en": {
            "subjects": [
                "Quick question about urgent enquiries",
                "Quick question about emergency intake",
                "Quick question about missed calls",
                "Quick question about after-hours enquiries",
            ],
            "observations": [
                "I noticed urgent or emergency care seems to be part of the clinic offer.",
                "It looks like the clinic handles cases where speed of response matters a lot.",
                "I saw emergency treatment mentioned, which usually makes intake timing more sensitive.",
                "It seems some enquiries may come in when a quick response matters more than usual.",
            ],
            "questions": [
                "If somebody reaches out after hours or misses the front desk, is that still handled manually?",
                "When an urgent enquiry comes in, do you already have a solid intake flow for it?",
                "Are missed calls and after-hours enquiries already covered well, or do some still slip through?",
                "When those enquiries come in, is the intake process already tight, or does it still depend on manual follow-up?",
            ],
        },
        "sk": {
            "subjects": [
                "Krátka otázka k urgentným dopytom",
                "Krátka otázka k emergency intake",
                "Krátka otázka k zmeškaným hovorom",
                "Krátka otázka k dopytom mimo ordinačných hodín",
            ],
            "observations": [
                "Všimol som si, že súčasťou ponuky sú aj urgentné alebo akútne ošetrenia.",
                "Vyzerá to, že pri niektorých prípadoch u vás zohráva rýchlosť reakcie dôležitú úlohu.",
                "Na webe som videl zmienku o urgentnom ošetrení, kde býva timing pri intake citlivý.",
                "Pôsobí to tak, že časť dopytov prichádza v situáciách, kde treba reagovať rýchlo.",
            ],
            "questions": [
                "Keď sa niekto ozve mimo ordinačných hodín alebo sa nedovolá, rieši sa to stále ručne?",
                "Keď príde urgentný dopyt, máte na to už spoľahlivý intake proces?",
                "Sú zmeškané hovory a dopyty mimo ordinačných hodín pokryté dobre, alebo sa niečo ešte stráca?",
                "Keď takéto dopyty prídu, je intake už dobre nastavený, alebo to stále závisí od manuálneho follow-upu?",
            ],
        },
        "cs": {
            "subjects": [
                "Krátký dotaz k urgentním poptávkám",
                "Krátký dotaz k emergency intake",
                "Krátký dotaz ke zmeškaným hovorům",
                "Krátký dotaz k poptávkám mimo ordinační dobu",
            ],
            "observations": [
                "Všiml jsem si, že součástí nabídky jsou i urgentní nebo akutní ošetření.",
                "Vypadá to, že u některých případů hraje rychlost reakce důležitou roli.",
                "Na webu jsem viděl zmínku o urgentním ošetření, kde bývá timing při intake citlivý.",
                "Působí to tak, že část poptávek přichází ve chvílích, kdy je potřeba reagovat rychle.",
            ],
            "questions": [
                "Když se někdo ozve mimo ordinační dobu nebo se nedovolá, řeší se to pořád ručně?",
                "Když přijde urgentní poptávka, máte na to už spolehlivý intake proces?",
                "Jsou zmeškané hovory a poptávky mimo ordinační dobu dobře pokryté, nebo něco ještě propadá?",
                "Když takové poptávky přijdou, je intake už dobře nastavený, nebo to stále závisí na manuálním follow-upu?",
            ],
        },
        "de": {
            "subjects": [
                "Kurze Frage zu dringenden Anfragen",
                "Kurze Frage zum Emergency Intake",
                "Kurze Frage zu verpassten Anrufen",
                "Kurze Frage zu Anfragen außerhalb der Öffnungszeiten",
            ],
            "observations": [
                "Mir ist aufgefallen, dass auch Notfall- oder Akutbehandlungen Teil Ihres Angebots sind.",
                "Es wirkt so, als ob bei manchen Fällen die Reaktionsgeschwindigkeit besonders wichtig ist.",
                "Auf der Website wird Notfallbehandlung erwähnt, was das Intake oft zeitkritischer macht.",
                "Ein Teil der Anfragen scheint in Situationen hereinzukommen, in denen schnelle Reaktion wichtig ist.",
            ],
            "questions": [
                "Wenn sich jemand außerhalb der Öffnungszeiten meldet oder niemanden erreicht, läuft das noch manuell?",
                "Wenn eine dringende Anfrage reinkommt, haben Sie dafür bereits einen verlässlichen Intake-Prozess?",
                "Sind verpasste Anrufe und Anfragen außerhalb der Öffnungszeiten schon gut abgedeckt, oder geht noch etwas verloren?",
                "Wenn solche Anfragen reinkommen, ist der Intake schon sauber gelöst, oder braucht es intern noch manuelles Nachfassen?",
            ],
        },
    },
}

NICHE_FALLBACK_PACKS = {
    "accounting_tax": {
        "en": {
            "subjects": [
                "Question on new client onboarding",
                "Question on collecting documents",
                "Quick one on new client admin",
                "Question on client intake",
            ],
            "openers": [
                "Whenever I hear from accountants, the back-and-forth at the start of a new client relationship comes up a lot.",
                "I keep hearing that the messy part is not the accounting itself, but getting everything in cleanly at the start.",
                "A few firms mentioned that the admin around onboarding tends to drag more than it should.",
                "From what I've seen, the first step with a new client can turn into a lot of chasing before the real work even starts.",
            ],
            "questions": [
                "When a new client comes in, is the intake flow already structured well, or does it still depend on manual chasing?",
                "When documents are missing, do reminders already run in a clean way, or is it still fairly manual?",
                "For a new client setup, do you already have a process you are happy with, or is there still a lot of back-and-forth?",
                "Would you say onboarding new clients is already smooth, or does document collection still eat up time?",
                "When new bookkeeping clients start, is the handoff already tight, or does it still depend on manual follow-up?",
                "Do missing documents and reminders already run through a solid process, or is that still pretty hands-on?",
                "When a client first signs on, is the intake flow already clear, or does it still need a lot of nudging?",
                "Is the first-step admin for new clients already under control, or does it still create avoidable chasing?",
            ],
        },
        "sk": {
            "subjects": [
                "Krátka otázka k onboardingu klientov",
                "Krátka otázka k zbieraniu dokumentov",
                "Krátka otázka k administratíve pri nástupe klienta",
                "Krátka otázka k intake nových klientov",
            ],
            "questions": [
                "Keď príde nový klient, máte intake nastavený dobre, alebo to stále závisí od ručného dohľadávania?",
                "Keď chýbajú dokumenty, fungujú pripomienky už hladko, alebo je to stále dosť manuálne?",
                "Máte onboarding nových klientov už vyriešený čisto, alebo je v tom ešte veľa follow-upu?",
                "Povedali by ste, že zber podkladov už beží hladko, alebo tím stále zbytočne zaťažuje?",
                "Keď sa zakladá nový klient, máte to už podchytené, alebo je okolo toho stále veľa naháňania?",
                "Funguje u vás prvý krok s novým klientom už bez trenia, alebo to ešte vytvára zbytočnú administratívu?",
                "Máte dohľadávanie chýbajúcich podkladov vyriešené, alebo je v tom stále dosť ručnej práce?",
                "Je onboarding nového klienta už pod kontrolou, alebo to stále stojí priveľa času?",
            ],
        },
        "cs": {
            "subjects": [
                "Krátký dotaz k onboardingu klientů",
                "Krátký dotaz ke sběru dokumentů",
                "Krátký dotaz k administrativě při nástupu klienta",
                "Krátký dotaz k intake nových klientů",
            ],
            "questions": [
                "Když přijde nový klient, máte intake už dobře nastavený, nebo to pořád závisí na ručním dohledávání?",
                "Když chybí dokumenty, fungují připomínky už hladce, nebo je to stále dost manuální?",
                "Máte onboarding nových klientů už vyřešený čistě, nebo je v tom pořád hodně follow-upu?",
                "Řekl byste, že sběr podkladů už běží hladce, nebo tým stále zbytečně zatěžuje?",
                "Když se zakládá nový klient, máte to už podchycené, nebo je kolem toho pořád hodně nahánění?",
                "Funguje u vás první krok s novým klientem už bez tření, nebo to stále vytváří zbytečnou administrativu?",
                "Máte dohledávání chybějících podkladů vyřešené, nebo je v tom stále dost ruční práce?",
                "Je onboarding nového klienta už pod kontrolou, nebo to pořád stojí příliš času?",
            ],
        },
        "de": {
            "subjects": [
                "Kurze Frage zum Mandanten-Onboarding",
                "Kurze Frage zur Dokumentensammlung",
                "Kurze Frage zum Intake neuer Mandanten",
                "Kurze Frage zum Onboarding-Aufwand",
            ],
            "questions": [
                "Wenn ein neuer Mandant startet, ist der Intake schon gut strukturiert, oder hängt es noch stark an manueller Nacharbeit?",
                "Laufen Erinnerungen bei fehlenden Unterlagen schon sauber, oder ist das noch recht manuell?",
                "Haben Sie für neues Mandanten-Onboarding bereits einen Ablauf, mit dem Sie zufrieden sind, oder gibt es noch viel Hin und Her?",
                "Würden Sie sagen, dass die Dokumentensammlung schon rund läuft, oder kostet sie intern noch unnötig Zeit?",
                "Ist der Einstieg neuer Mandanten schon sauber gelöst, oder braucht es noch viel manuelles Nachfassen?",
                "Sind fehlende Unterlagen bei Ihnen schon gut organisiert, oder entsteht dabei noch unnötiger Aufwand?",
                "Ist der erste Schritt mit neuen Mandanten schon unter Kontrolle, oder erzeugt das noch vermeidbare Nacharbeit?",
                "Funktioniert das Onboarding bereits zuverlässig, oder hängt noch zu viel an manuellem Hinterhergehen?",
            ],
        },
    },
    "real_estate": {
        "en": {
            "subjects": [
                "3-day reply time",
                "property enquiry question",
                "quick one on replies",
                "lead response question",
                "new enquiry question",
                "follow-up question",
            ],
            "openers": [
                "I was looking at properties recently and a few agents took 3+ days to reply, so I ended up following up myself.",
                "I was browsing listings recently and more than once I had to chase the agent just to get a reply.",
                "I was checking properties not long ago and a few enquiries just sat there until I nudged again.",
                "I was looking around recently and the reply times on some listings were honestly pretty slow.",
                "I was enquiring on a few properties recently and I ended up following up myself more than once.",
                "I looked at a few properties recently and some of the replies came only after I pushed again.",
            ],
            "questions": [
                "Made me curious: do you handle new enquiries manually too?",
                "Can I ask if new property enquiries are still handled manually on your side too?",
                "Do you still manage the first response manually, or have you already tightened that up?",
                "When a fresh enquiry comes in, is that still pretty manual for you too?",
                "Do new listing enquiries still get handled case by case, or do you already have a system for it?",
                "Is the first response still something the team handles manually, or not anymore?",
            ],
        },
        "sk": {
            "subjects": [
                "3-dňová odozva",
                "otázka ohľadom odpovedania na potenciálnych zákazníkov",
                "otázka k novým dopytom",
                "krátko k odpovediam",
                "otázka ohľadom nových záujemcov",
            ],
            "openers": [
                "Nedávno som pozeral nehnuteľnosti a pri pár makléroch odpoveď prišla až po 3+ dňoch, takže som sa musel pripomenúť sám.",
                "Keď som nedávno riešil pár nehnuteľností, viackrát som musel follow-upovať sám, aby som vôbec dostal odpoveď.",
                "Nedávno som písal na pár inzerátov a niektoré dopyty ostali visieť, kým som sa neozval znova.",
                "Pri pozeraní nehnuteľností som si nedávno všimol, že odpoveď na dopyt vie prísť dosť neskoro.",
            ],
            "questions": [
                "Napadlo mi: riešite nové dopyty ešte manuálne aj u vás?",
                "Môžem sa spýtať, či nové dopyty na nehnuteľnosti riešite ešte ručne aj vy?",
                "Je prvá reakcia na nový dopyt ešte stále manuálna, alebo to už máte podchytené?",
                "Keď príde čerstvý dopyt, ide to u vás ešte dosť ručne, alebo už nie?",
                "Riešia sa nové enquiry ešte prípad od prípadu, alebo už na to máte systém?",
            ],
        },
    },
}


def extract_name(about_text: str, email: str = "") -> str:
    """Best-effort extraction from email prefix. Skip generic inboxes."""
    if email:
        prefix = email.split("@")[0].lower()
        generic = {
            "info", "contact", "hello", "admin", "support", "office", "mail", "team",
            "sales", "enquiries", "enquiry", "noreply", "reception", "recepcia",
            "klinika", "clinic", "dental", "praxis", "ordination", "booking",
        }
        if prefix not in generic and re.match(r"^[a-z]+\.[a-z]+$", prefix):
            first, _last = prefix.split(".", 1)
            if any(token in first for token in ("dent", "clinic", "smile", "praxis", "centrum", "center", "zahn")):
                return ""
            return first.capitalize()
    return ""


def _normalize_language(raw: str) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return "en"
    if value in LANG_ALIASES:
        return LANG_ALIASES[value]
    for part in re.split(r"[\s,;/()_-]+", value):
        if part in LANG_ALIASES:
            return LANG_ALIASES[part]
    if value.startswith("sk"):
        return "sk"
    if value.startswith("cs") or value.startswith("cz"):
        return "cs"
    if value.startswith("de") or "german" in value:
        return "de"
    return "en"


def _stable_index(*parts: object) -> int:
    source = "|".join(str(part or "") for part in parts)
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _pick(options: list[str], seed: int, salt: str = "") -> str:
    if not options:
        return ""
    idx = _stable_index(seed, salt) % len(options)
    return options[idx]


def _greeting(language: str, contact_name: str, seed: int) -> str:
    pack = LANG_PACKS.get(language, LANG_PACKS["en"])
    if contact_name:
        return pack["greeting_named"].format(name=contact_name)
    return _pick(pack.get("greeting_options") or [pack["greeting_plain"]], seed, "greeting")


def _infer_language(lead: dict, normalized: str) -> str:
    if normalized and normalized != "en":
        return normalized
    website = (lead.get("website") or lead.get("site_url") or "").lower()
    address = (lead.get("address") or "").lower()
    if ".sk" in website or "bratislava" in address or "slovakia" in address or "slovensko" in address:
        return "sk"
    if ".cz" in website or "praha" in address or "brno" in address or "czech" in address or "čes" in address:
        return "cs"
    if ".at" in website or ".de" in website or "wien" in address or "vienna" in address or "deutsch" in address:
        return "de"
    return normalized or "en"


def _generic_email(language: str, greeting: str, business_name: str, top_gap: str, gap_observation: str, seed: int) -> dict:
    pack = LANG_PACKS.get(language, LANG_PACKS["en"])
    subject = DEFAULT_SUBJECTS.get(top_gap) or pack["fallback_subject"] or f"Quick question about {business_name}"
    body = "\n\n".join(
        [
            greeting,
            _pick(pack["fallback_body"], seed, "fallback_body") if not gap_observation else gap_observation.rstrip(".") + ".",
            _pick(pack["fallback_question"], seed, "fallback_question"),
            _pick(pack["close"], seed, "close"),
        ]
    )
    return {"subject": subject[:80], "body": body, "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]}


def _minimal_question_email(language: str, greeting: str, content: dict, seed: int) -> dict:
    subject = _pick(content["subjects"], seed, "minimal_subject")
    opener = _pick(content.get("openers", []), seed, "minimal_opener")
    question = _pick(content["questions"], seed, "minimal_question")
    lines = [greeting]
    if opener:
        lines.append(opener)
    lines.append(question)
    body = "\n\n".join(lines)
    return {"subject": subject[:80], "body": body, "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]}


def _soft_offer_email(language: str, greeting: str, content: dict, seed: int) -> dict:
    subject = _pick(content["subjects"], seed, "soft_offer_subject")
    opener = _pick(content.get("openers", []), seed, "soft_offer_opener")
    question = _pick(content["questions"], seed, "soft_offer_question")
    lines = [greeting]
    if opener:
        lines.append(opener)
    lines.append(question)
    body = "\n\n".join(lines)
    return {"subject": subject[:80], "body": body, "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]}


def _opportunity_email(language: str, greeting: str, business_name: str, top_opportunity: str, outreach_angle: str, seed: int) -> dict:
    lang = language if language in LANG_PACKS else "en"
    opp_pack = OPPORTUNITY_PACKS.get(top_opportunity, {})
    content = opp_pack.get(lang) or opp_pack.get("en")
    if not content:
        return _generic_email(lang, greeting, business_name, "", outreach_angle, seed)

    close_pack = LANG_PACKS.get(lang, LANG_PACKS["en"])
    subject = _pick(content["subjects"], seed, "subject")
    observation = _pick(content["observations"], seed, "observation")
    question = _pick(content["questions"], seed, "question")
    closer = _pick(close_pack["close"], seed, "close")

    body = "\n\n".join([greeting, observation, question, closer])
    return {"subject": subject[:80], "body": body, "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]}


def generate_email(lead: dict) -> dict:
    """Return {'subject': str, 'body': str} for email outreach."""
    email_addr = (lead.get("site_emails") or lead.get("email_maps") or "").split(",")[0].strip()
    contact_name = extract_name(lead.get("brand_summary", ""), email_addr)
    seed = _stable_index(
        lead.get("id"),
        lead.get("name"),
        lead.get("top_opportunity"),
        lead.get("top_gap"),
        lead.get("language", ""),
    )
    language = _infer_language(lead, _normalize_language(lead.get("language", "")))
    greeting = _greeting(language, contact_name, seed)

    top_opportunity = lead.get("top_opportunity") or ""
    if top_opportunity:
        return _opportunity_email(
            language,
            greeting,
            lead.get("name", ""),
            top_opportunity,
            lead.get("outreach_angle", ""),
            seed,
        )

    target_niche = (lead.get("target_niche") or "").strip()
    niche_pack = NICHE_FALLBACK_PACKS.get(target_niche, {})
    content = niche_pack.get(language) or niche_pack.get("en")
    if content:
        if target_niche in {"real_estate", "accounting_tax"}:
            framework = _pick(["minimal_question", "soft_offer"], seed, f"{target_niche}_framework")
            if framework == "soft_offer":
                return _soft_offer_email(language, greeting, content, seed)
        if "observations" not in content:
            return _minimal_question_email(language, greeting, content, seed)
        close_pack = LANG_PACKS.get(language, LANG_PACKS["en"])
        subject = _pick(content["subjects"], seed, "niche_subject")
        observation = _pick(content["observations"], seed, "niche_observation")
        question = _pick(content["questions"], seed, "niche_question")
        closer = _pick(close_pack["close"], seed, "close")
        body = "\n\n".join([greeting, observation, question, closer])
        return {"subject": subject[:80], "body": body, "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]}

    return _generic_email(
        language,
        greeting,
        lead.get("name", ""),
        lead.get("top_gap", ""),
        lead.get("outreach_angle", ""),
        seed,
    )


def generate_dm(lead: dict, channel: str = "instagram") -> str:
    email = generate_email(lead)
    lines = [line.strip() for line in email["body"].splitlines() if line.strip()]
    dm_lines = lines[1:3] if len(lines) >= 3 else lines
    return " ".join(dm_lines)[:350]
