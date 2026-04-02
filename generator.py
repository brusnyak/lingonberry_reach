"""
outreach/generator.py
Deterministic outreach draft generator with language-aware variations.

Philosophy (from material/):
- Plain text only. No HTML, no formatting.
- Under 150 words. Ideally under 80.
- One CTA. Zero pressure.
- Sound like a human, not a cold email.
- Direct: state what you do, what you can do for them specifically, ask one question.
- Structure: greeting → what I do + specific value for their situation → soft CTA → sign-off
"""
from __future__ import annotations

import hashlib
import os
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
        "greeting_options": ["Hi,", "Hello,", "Hey there,", "Hi there,", "Hey,", "Greetings,"],
        "close": [
            "Happy to keep this brief.",
            "No need for a long reply.",
            "Happy to keep it short.",
            "A quick reply would be plenty.",
            "Cheers,",
            "Best,",
            "Thanks,",
        ],
        "soft_close": [
            "Let me know.",
            "Curious either way.",
            "Would be good to know.",
            "Open to a quick reply.",
        ],
        "ps_options": [
            "i use it myself, just curious if it could help you too",
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
        "greeting_named": "Ahoj {name},",
        "greeting_plain": "Ahoj,",
        "greeting_options": ["Ahoj,", "Zdravím,", "Dobrý deň,"],
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
        "greeting_named": "Hallo {name},",
        "greeting_plain": "Hallo,",
        "greeting_options": ["Hallo,", "Servus,", "Guten Tag,"],
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
    """Best-effort first name extraction from email prefix. Skip generic inboxes."""
    if not email:
        return ""
    prefix = email.split("@")[0].lower()
    domain = email.split("@")[1].lower() if "@" in email else ""
    domain_root = domain.split(".")[0]  # e.g. "gwent" from "gwent.sk"

    generic = {
        "info", "contact", "hello", "hallo", "bonjour", "admin", "support", "office",
        "noffice", "mail", "team", "sales", "enquiries", "enquiry", "noreply",
        "reception", "rezeption", "recepce", "recepcia", "klinika", "clinic", "dental",
        "praxis", "ordination", "booking", "bookings", "accounts", "jobs", "work",
        "service", "services", "help", "general", "workorders", "welcome",
        "termine", "termin", "kontakt", "kosice", "bratislava", "vienna", "wien", "prague",
        "webmaster", "postmaster", "hostmaster", "root", "user", "marketing", "admin",
    }
    if prefix in generic:
        return ""

    # Skip if prefix matches the domain root — it's a business name, not a person
    if prefix == domain_root or prefix.startswith(domain_root) or domain_root.startswith(prefix):
        return ""

    # Skip if prefix contains obvious business/service tokens — but only if the prefix
    # is long enough that the token isn't just a coincidental substring of a real name.
    # e.g. "meliplumbingservices" contains "plumb" → skip (business name)
    # but "charlie" doesn't contain any token → keep
    _BIZ_TOKENS = ("dent", "clinic", "smile", "praxis", "centrum", "center", "zahn",
                   "plumb", "electr", "hvac", "account", "legal", "realt", "estate",
                   "beauty", "salon", "studio", "group", "agency", "media", "digital",
                   "service", "solution", "consult", "manage", "invest", "property")
    # Only apply token filter if prefix is longer than a typical first name (>8 chars)
    if len(prefix) > 8 and any(t in prefix for t in _BIZ_TOKENS):
        return ""

    # firstname.lastname@ → "Firstname"
    if re.match(r"^[a-z]{2,15}\.[a-z]{2,15}$", prefix):
        return prefix.split(".")[0].capitalize()

    # Plain short first name: james@, sarah@, jackson@ (3-10 chars, all alpha)
    if re.match(r"^[a-z]{3,10}$", prefix):
        return prefix.capitalize()

    # firstnamelastname@ — split only for longer prefixes (>10 chars)
    # e.g. egorbrusnyak → "Egor", jakubnovak → "Jakub", meliplumbing → "Meli"
    if re.match(r"^[a-z]{11,20}$", prefix):
        for length in range(4, 9):
            candidate = prefix[:length]
            rest = prefix[length:]
            if len(rest) >= 3 and re.match(r"^[a-z]+$", rest):
                return candidate.capitalize()

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
    mode = os.environ.get("OUTREACH_LANGUAGE_MODE", "english_first").strip().lower()
    if mode in {"english", "english_first", "en"}:
        return "en"
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



# ── Direct-style packs (new philosophy) ──────────────────────────────────────
# Structure: greeting → what I do + specific value for their niche → soft CTA → sign-off
# Under 80 words. Plain. Human. One question at the end.

DIRECT_SUBJECTS = {
    "home_services": {
        "en": ["quick question", "quick question about enquiries", "missed calls question", "booking question", "quick one on admin"],
    },
    "real_estate": {
        "en": ["quick question about enquiries", "buyer enquiry handling", "lead follow-up question", "property enquiry flow"],
        "sk": ["krátka otázka", "3-dňová odozva", "otázka k dopytom", "otázka k záujemcom"],
        "cs": ["krátký dotaz", "3denní odezva", "dotaz k poptávkám", "dotaz k zájemcům"],
        "de": ["kurze Frage", "3-Tage-Reaktionszeit", "Frage zu Anfragen", "Frage zu Interessenten"],
    },
    "accounting_tax": {
        "en": ["quick question", "document chasing question", "client onboarding question", "quick one on admin"],
        "sk": ["krátka otázka", "otázka k podkladom", "otázka k onboardingu", "krátko k administratíve"],
        "cs": ["krátký dotaz", "dotaz k podkladům", "dotaz k onboardingu", "krátce k administrativě"],
        "de": ["kurze Frage", "Frage zu Unterlagen", "Frage zum Onboarding", "kurz zur Verwaltung"],
    },
    "dental_medical": {
        "en": ["quick question", "patient enquiry question", "booking follow-up question", "quick one on intake"],
        "sk": ["krátka otázka", "otázka k dopytom pacientov", "otázka k rezerváciám", "krátko k intake"],
        "cs": ["krátký dotaz", "dotaz k poptávkám pacientů", "dotaz k rezervacím", "krátce k intake"],
        "de": ["kurze Frage", "Frage zu Patientenanfragen", "Frage zu Buchungen", "kurz zum Intake"],
    },
    "_default": {
        "en": ["quick question", "quick one on follow-up", "quick one on admin"],
        "sk": ["krátka otázka", "krátko k follow-upu", "krátko k administratíve"],
        "cs": ["krátký dotaz", "krátce k follow-upu", "krátce k administrativě"],
        "de": ["kurze Frage", "kurz zum Follow-up", "kurz zur Verwaltung"],
    },
}

# What I do — one sentence, plain
WHAT_I_DO = {
    "en": [
        "I help people save time and make more money with simple automations.",
        "I build automations that help small businesses save time and close more deals.",
        "I help business owners get more done without hiring more people.",
    ],
    "sk": [
        "Pomáham ľuďom ušetriť čas a zarobiť viac s jednoduchými automatizáciami.",
        "Staviam automatizácie, ktoré pomáhajú malým firmám ušetriť čas a uzatvoriť viac obchodov.",
        "Pomáham majiteľom firiem tráviť menej času administratívou a viac skutočnou prácou.",
    ],
    "cs": [
        "Pomáhám lidem ušetřit čas a vydělat více s jednoduchými automatizacemi.",
        "Stavím automatizace, které pomáhají malým firmám ušetřit čas a uzavřít více obchodů.",
        "Pomáhám majitelům firem trávit méně času administrativou a více skutečnou prací.",
    ],
    "de": [
        "Ich helfe Menschen, Zeit zu sparen und mehr zu verdienen – mit einfachen Automatisierungen.",
        "Ich baue Automatisierungen, die kleinen Unternehmen helfen, Zeit zu sparen und mehr Abschlüsse zu erzielen.",
        "Ich helfe Unternehmern, weniger Zeit mit Verwaltung zu verbringen und mehr mit echter Arbeit.",
    ],
}

# Specific value by niche — what I could do for them
NICHE_VALUE = {
    "home_services": {
        "en": [
            "I build simple systems that handle initial enquiries and book qualified jobs straight into the calendar so nothing gets missed while you're on site.",
            "I set up automatic text and email responses for missed calls so every enquiry gets an immediate answer and the best jobs don't go to someone faster.",
            "I build simple follow-up flows that keep the conversation warm with new leads until they are ready to book, without you needing to manually chase.",
        ],
    },
    "real_estate": {
        "en": [
            "For a real estate agent I could automate follow-up on new enquiries so no lead goes cold while you're showing properties.",            "For an agent I could set up automatic replies and follow-up sequences so every new enquiry gets a response within minutes.",
            "For a real estate agent I could automate the first response and follow-up so you stop losing leads to slow reply times.",
        ],
        "sk": [
            "Pre makléra by som mohol automatizovať follow-up na nové dopyty, aby žiadny záujemca nevychladol, kým ste na obhliadke.",
            "Pre makléra by som mohol nastaviť automatické odpovede a follow-up sekvencie, aby každý nový dopyt dostal odpoveď do pár minút.",
            "Pre realitného makléra by som mohol automatizovať prvú odpoveď a follow-up, aby ste neprichádzali o záujemcov kvôli pomalej odozve.",
        ],
        "cs": [
            "Pro makléře bych mohl automatizovat follow-up na nové poptávky, aby žádný zájemce nevychladl, zatímco jste na prohlídce.",
            "Pro makléře bych mohl nastavit automatické odpovědi a follow-up sekvence, aby každá nová poptávka dostala odpověď do pár minut.",
            "Pro realitního makléře bych mohl automatizovat první odpověď a follow-up, abyste nepřicházeli o zájemce kvůli pomalé odezvě.",
        ],
        "de": [
            "Für einen Makler könnte ich das Follow-up bei neuen Anfragen automatisieren, damit kein Lead kalt wird, während Sie Besichtigungen machen.",
            "Für einen Makler könnte ich automatische Antworten und Follow-up-Sequenzen einrichten, damit jede neue Anfrage innerhalb von Minuten eine Antwort bekommt.",
            "Für einen Immobilienmakler könnte ich die erste Antwort und das Follow-up automatisieren, damit Sie keine Leads mehr durch langsame Reaktionszeiten verlieren.",
        ],
    },
    "accounting_tax": {
        "en": [
            "For an accountant I could automate document collection and reminders so you stop chasing clients for missing files.",
            "For an accounting firm I could set up automated onboarding flows so new clients get everything they need without manual back-and-forth.",
            "For an accountant I could automate the document chase so your team spends more time on actual work.",
        ],
        "sk": [
            "Pre účtovníka by som mohol automatizovať zbieranie dokumentov a pripomienky, aby ste prestali naháňať klientov za chýbajúcimi podkladmi.",
            "Pre účtovnícku firmu by som mohol nastaviť automatizovaný onboarding, aby noví klienti dostali všetko bez manuálneho back-and-forth.",
            "Pre účtovníka by som mohol automatizovať naháňanie dokumentov, aby váš tím trávil menej času administratívou.",
        ],
        "cs": [
            "Pro účetního bych mohl automatizovat sběr dokumentů a připomínky, abyste přestali honit klienty za chybějícími podklady.",
            "Pro účetní firmu bych mohl nastavit automatizovaný onboarding, aby noví klienti dostali vše bez manuálního back-and-forth.",
            "Pro účetního bych mohl automatizovat honění dokumentů, aby váš tým trávil méně času administrativou.",
        ],
        "de": [
            "Für einen Steuerberater könnte ich die Dokumentensammlung und Erinnerungen automatisieren, damit Sie aufhören, Mandanten nach fehlenden Unterlagen zu jagen.",
            "Für eine Steuerkanzlei könnte ich automatisierte Onboarding-Abläufe einrichten, damit neue Mandanten alles bekommen, ohne manuelles Hin und Her.",
            "Für einen Steuerberater könnte ich das Dokumenten-Hinterherlaufen automatisieren, damit Ihr Team weniger Zeit mit Verwaltung verbringt.",
        ],
    },
    "dental": {
        "en": [
            "For a dental clinic I could automate appointment reminders and follow-up on missed enquiries so fewer patients slip through.",
            "For a clinic I could set up automated intake and reminder flows so your front desk spends less time on manual chasing.",
            "For a dental practice I could automate the first response to new enquiries and no-show follow-up so nothing falls through the cracks.",
        ],
        "sk": [
            "Pre zubnú kliniku by som mohol automatizovať pripomienky termínov a follow-up na zmeškané dopyty, aby menej pacientov vypadlo.",
            "Pre kliniku by som mohol nastaviť automatizovaný intake a pripomienky, aby recepcia trávila menej času manuálnym naháňaním.",
            "Pre zubnú prax by som mohol automatizovať prvú odpoveď na nové dopyty a follow-up po nezjaveniam, aby nič nevypadlo.",
        ],
        "cs": [
            "Pro zubní kliniku bych mohl automatizovat připomínky termínů a follow-up na zmeškaných poptávky, aby méně pacientů vypadlo.",
            "Pro kliniku bych mohl nastavit automatizovaný intake a připomínky, aby recepce trávila méně času manuálním nahánění.",
            "Pro zubní praxi bych mohl automatizovat první odpověď na nové poptávky a follow-up po nedostavení, aby nic nevypadlo.",
        ],
        "de": [
            "Für eine Zahnarztpraxis könnte ich Terminerinnerungen und Follow-up bei verpassten Anfragen automatisieren, damit weniger Patienten verloren gehen.",
            "Für eine Praxis könnte ich automatisierte Intake- und Erinnerungsabläufe einrichten, damit die Rezeption weniger Zeit mit manuellem Nachfassen verbringt.",
            "Für eine Zahnarztpraxis könnte ich die erste Antwort auf neue Anfragen und das No-Show-Follow-up automatisieren, damit nichts durchs Raster fällt.",
        ],
    },
    "_default": {
        "en": [
            "I could automate the repetitive parts of your workflow so your team spends less time on admin.",
            "I could set up simple automations to handle follow-up, reminders, and intake so nothing falls through the cracks.",
            "I could automate the manual back-and-forth in your process so you get more done with less effort.",
        ],
        "sk": [
            "Mohol by som automatizovať opakujúce sa časti vášho procesu, aby váš tím trávil menej času administratívou.",
            "Mohol by som nastaviť jednoduché automatizácie na follow-up, pripomienky a intake, aby nič nevypadlo.",
            "Mohol by som automatizovať manuálny back-and-forth vo vašom procese, aby ste zvládli viac s menším úsilím.",
        ],
        "cs": [
            "Mohl bych automatizovat opakující se části vašeho procesu, aby váš tým trávil méně času administrativou.",
            "Mohl bych nastavit jednoduché automatizace pro follow-up, připomínky a intake, aby nic nevypadlo.",
            "Mohl bych automatizovat manuální back-and-forth ve vašem procesu, abyste zvládli více s menším úsilím.",
        ],
        "de": [
            "Ich könnte die sich wiederholenden Teile Ihres Prozesses automatisieren, damit Ihr Team weniger Zeit mit Verwaltung verbringt.",
            "Ich könnte einfache Automatisierungen für Follow-up, Erinnerungen und Intake einrichten, damit nichts durchs Raster fällt.",
            "Ich könnte das manuelle Hin und Her in Ihrem Prozess automatisieren, damit Sie mehr mit weniger Aufwand erledigen.",
        ],
    },
}

# Soft CTA — one question, zero pressure
SOFT_CTA = {
    "en": [
        "Would you be interested?",
        "Does that sound useful?",
        "Worth a quick chat?",
        "Curious if that's relevant for you.",
    ],
    "sk": [
        "Zaujalo by vás to?",
        "Dáva to zmysel pre vás?",
        "Stojí to za krátky rozhovor?",
        "Zaujíma ma, či je to pre vás relevantné.",
    ],
    "cs": [
        "Zaujalo by vás to?",
        "Dává to smysl pro vás?",
        "Stojí to za krátký rozhovor?",
        "Zajímá mě, jestli je to pro vás relevantní.",
    ],
    "de": [
        "Wäre das interessant für Sie?",
        "Klingt das nützlich?",
        "Wäre ein kurzes Gespräch sinnvoll?",
        "Mich würde interessieren, ob das für Sie relevant ist.",
    ],
}

SIGN_OFF = {
    "en": "Let me know,\n{name}",
    "sk": "Dajte vedieť,\n{name}",
    "cs": "Dejte vědět,\n{name}",
    "de": "Geben Sie gern Bescheid,\n{name}",
}

# ── Touch 1 copy ──────────────────────────────────────────────────────────────
# Plain, open first touch. Sender can say they're starting out. Sign-off added later.
# Structure: greeting → plain intro → specific thing → one question → (sign-off added at send time)

TOUCH1_INTRO = {
    "en": [
        "New enquiries get handled in minutes instead of sitting unanswered.",
        "Missed calls and late replies stop leaking booked jobs.",
        "Lead qualification and first responses run automatically while you're on site.",
        "Qualified jobs can be routed straight to your calendar without manual back-and-forth.",
    ],
    "sk": [
        "Začínam pomáhať menším firmám šetriť čas pomocou jednoduchých automatizácií.",
        "Začínam s jednoduchými automatizáciami pre menšie firmy.",
    ],
    "cs": [
        "Začínám pomáhat menším firmám šetřit čas pomocí jednoduchých automatizací.",
        "Začínám s jednoduchými automatizacemi pro menší firmy.",
    ],
    "de": [
        "Ich starte gerade damit, kleinen Unternehmen mit einfachen Automatisierungen Zeit zu sparen.",
        "Ich fange gerade mit einfachen Automatisierungen für kleinere Unternehmen an.",
    ],
}

TOUCH1_SPECIFIC = {
    "home_services": {
        "en": [
            "Every new lead gets a fast first response and basic qualification before your phone even comes out.",
            "Reply time drops to minutes, so good jobs stop going to whoever answered first.",
            "Initial back-and-forth is handled automatically and booking-ready leads are surfaced immediately.",
            "Low-intent enquiries are filtered out so your time goes to serious jobs only.",
        ],
    },
    "real_estate": {
        "en": [
            "New buyer enquiries can get immediate replies and follow-up without agents manually chasing every lead.",
            "Serious buyers get handled faster with enquiry and follow-up workflows built for agency speed.",
            "Inbound leads can be qualified and followed up automatically so the team stays focused on live deals.",
            "For real estate agencies, that usually means faster replies on new buyer enquiries, basic qualification, and less manual chasing across the team.",
        ],
        "sk": [
            "Pomáham realitným kanceláriám riešiť nové dopyty rýchlejšou prvou odpoveďou a čistejším follow-upom.",
            "Robím jednoduché enquiry a follow-up workflowy pre realitné kancelárie, aby dopyty nezostávali visieť.",
        ],
        "cs": [
            "Pomáhám realitním kancelářím řešit nové poptávky rychlejší první odpovědí a čistším follow-upem.",
            "Dělám jednoduché enquiry a follow-up workflowy pro realitní kanceláře, aby poptávky nezůstávaly viset.",
        ],
        "de": [
            "Ich helfe Immobilienbüros bei neuen Anfragen mit einer schnelleren ersten Antwort und saubererem Follow-up.",
            "Ich baue einfache Anfrage- und Follow-up-Abläufe für Immobilienbüros, damit Leads nicht liegen bleiben.",
        ],
    },
    "accounting_tax": {
        "en": [
            "Document chasing can run on autopilot so clients get reminders without manual follow-up.",
            "Client onboarding and document collection can be made much less manual.",
        ],
        "sk": [
            "Myslím, že by som vedel pomôcť s naháňaním podkladov, aby klienti dostávali pripomienky bez ručného follow-upu.",
            "Myslím, že by som vedel pomôcť spraviť onboarding a zber dokumentov menej manuálny.",
        ],
        "cs": [
            "Myslím, že bych uměl pomoct s naháněním podkladů, aby klienti dostávali připomínky bez ručního follow-upu.",
            "Myslím, že bych uměl pomoct udělat onboarding a sběr dokumentů méně manuální.",
        ],
        "de": [
            "Ich glaube, ich könnte beim Nachfassen fehlender Unterlagen helfen, damit Sie nicht alles manuell erinnern müssen.",
            "Ich glaube, ich könnte Onboarding und Unterlagensammlung etwas weniger manuell machen.",
        ],
    },
    "dental_medical": {
        "en": [
            "New patient enquiries can get a quick first response instead of waiting in queue.",
            "Intake follow-up can run automatically so fewer enquiries go quiet.",
        ],
        "sk": [
            "Myslím, že by som vedel pomôcť s novými dopytmi pacientov, aby ľudia dostali rýchlejšiu odpoveď.",
            "Myslím, že by som vedel pomôcť s intake follow-upom, aby menej dopytov ostalo bez reakcie.",
        ],
        "cs": [
            "Myslím, že bych uměl pomoct s novými poptávkami pacientů, aby lidé dostali rychlejší odpověď.",
            "Myslím, že bych uměl pomoct s intake follow-upem, aby méně poptávek zůstalo bez reakce.",
        ],
        "de": [
            "Ich glaube, ich könnte bei neuen Patientenanfragen helfen, damit Menschen schneller eine Antwort bekommen.",
            "Ich glaube, ich könnte beim Intake-Follow-up helfen, damit weniger Anfragen ohne Reaktion bleiben.",
        ],
    },
    "_default": {
        "en": [
            "Manual back-and-forth around enquiries and admin can be reduced fast.",
            "New enquiries and follow-up can be handled with far less manual effort.",
        ],
        "sk": [
            "Myslím, že by som vedel pomôcť ubrať z manuálneho back-and-forth okolo dopytov a administratívy.",
            "Myslím, že by som vedel pomôcť ušetriť čas v tom, ako riešite nové dopyty a follow-up.",
        ],
        "cs": [
            "Myslím, že bych uměl pomoct ubrat z manuálního back-and-forth kolem poptávek a administrativy.",
            "Myslím, že bych uměl pomoct ušetřit čas v tom, jak řešíte nové poptávky a follow-up.",
        ],
        "de": [
            "Ich glaube, ich könnte etwas von dem manuellen Hin und Her bei Anfragen und Verwaltung abnehmen.",
            "Ich glaube, ich könnte dabei helfen, bei neuen Anfragen und Follow-up etwas Zeit zu sparen.",
        ],
    },
}

# UK Trades A/B Test Variants — Version A (Pain + Quick Value) and Version B (Benefit + Question)
# 50/50 split — track open rate + reply rate
UK_TRADES_AB = {
    "variant_a": {
        "subject": "Stop missing job enquiries while on site?",
        "body_template": """Hi {name},

New enquiries from {platform} often go unanswered while you're working — and you lose good jobs.

A simple workflow filters junk, drafts replies in your style, and books qualified leads into your calendar.

You only get pinged for the hot ones.

Running 7-day free tests now at low rate for first setups.

Want a quick Loom?""",
        "word_count": 52,
    },
    "variant_b": {
        "subject": "Save 2-3 hours a week on enquiries?",
        "body_template": """Hi {name},

Initial back-and-forth on new job enquiries can be handled automatically, with qualified jobs routed into your calendar.

Everything stays in your email — you approve replies.

Doing 7-day free tests for a few UK tradies at low rate.

Open to seeing a quick Loom of how it works?""",
        "word_count": 48,
    },
}

# Platform detection for personalization
UK_PLATFORMS = ["Checkatrade", "MyBuilder", "Bark", "TrustATrader", "Rated People", "Checkatrade or MyBuilder"]

# UK Trades specific follow-up templates
UK_TRADES_FOLLOWUP = {
    "touch2": """Hi {name},

Did you see my note about handling job enquiries?

Still happy to run the free 7-day test — no commitment needed.

Cheers,
{sender_name}""",
    "touch3": """Hi {name},

Just one more nudge on this.

The 7-day free test is still open if you want to see how it works.

Cheers,
{sender_name}""",
    "touch4": """Hi {name},

Last one from me.

If handling enquiries better isn't a priority right now, no worries at all.

Cheers,
{sender_name}""",
    "touch5": """Hi {name},

I'll stop here — but if you ever want to revisit, just reply to this thread.

Cheers,
{sender_name}""",
}

TOUCH1_CTA = {
    "en": [
        "Worth a quick look this week?",
        "Open to a 10-minute walkthrough?",
        "If this gave you 2-3 hours back weekly, would that be useful?",
        "Useful for your setup? Reply yes or no.",
    ],
    "sk": [
        "Zaujalo by vás to?",
        "Dáva to zmysel?",
        "Je to pre vás relevantné?",
        "Stojí to za krátky rozhovor?",
    ],
    "cs": [
        "Zaujalo by vás to?",
        "Dává to smysl?",
        "Je to pro vás relevantní?",
        "Stojí to za krátký rozhovor?",
    ],
    "de": [
        "Wäre das interessant?",
        "Klingt das nützlich?",
        "Ist das relevant für Sie?",
        "Wäre ein kurzes Gespräch sinnvoll?",
    ],
}

# ── Touch 2-5 copy ────────────────────────────────────────────────────────────

TOUCH2 = {
    "en": [
        "Hey {name}, just checking this landed.\n\nStill happy to help if the timing's off.",
        "Hey {name} — did this reach you?\n\nHappy to keep it short if you want.",
        "{name} — just a quick follow-up in case this got buried.",
    ],
    "sk": [
        "{name}, len sa uisťujem, že správa prišla.\n\nStále rád pomôžem, ak čas nevyšiel.",
        "{name} — dostali ste túto správu?\n\nKľudne len krátko, ak chcete.",
        "{name} — krátky follow-up, ak sa správa stratila.",
    ],
    "cs": [
        "{name}, jen se ujišťuji, že zpráva dorazila.\n\nStále rád pomůžu, pokud čas nevyšel.",
        "{name} — dostali jste tuto zprávu?\n\nKlidně jen krátce, pokud chcete.",
        "{name} — krátký follow-up, pokud se zpráva ztratila.",
    ],
    "de": [
        "{name}, ich wollte nur sichergehen, dass die Nachricht angekommen ist.\n\nHelfe gerne, falls der Zeitpunkt nicht gepasst hat.",
        "{name} — haben Sie diese Nachricht erhalten?\n\nGerne auch kurz, wenn Sie möchten.",
        "{name} — kurzes Follow-up, falls die Nachricht untergegangen ist.",
    ],
}

TOUCH3 = {
    "en": [
        "{name} — still happy to help if the timing's off.",
        "One more try, {name}. No pressure.",
        "{name} — just staying visible in case it's useful later.",
    ],
    "sk": [
        "{name} — stále rád pomôžem, ak čas nevyšiel.",
        "Ešte jeden pokus, {name}. Bez tlaku.",
        "{name} — len zostávam viditeľný, ak by sa to hodilo neskôr.",
    ],
    "cs": [
        "{name} — stále rád pomůžu, pokud čas nevyšel.",
        "Ještě jeden pokus, {name}. Bez tlaku.",
        "{name} — jen zůstávám viditelný, pokud by se to hodilo později.",
    ],
    "de": [
        "{name} — helfe gerne, falls der Zeitpunkt nicht gepasst hat.",
        "Noch ein Versuch, {name}. Kein Druck.",
        "{name} — ich bleibe einfach sichtbar, falls es später nützlich ist.",
    ],
}

TOUCH4 = {
    "en": [
        "One last try, {name}.\n\n{specific}.\n\nIf it's not relevant, no worries at all.",
        "{name} — last one from me.\n\n{specific}.\n\nNo worries if not.",
    ],
    "sk": [
        "Posledný pokus, {name}.\n\n{specific}.\n\nAk to nie je relevantné, žiadny problém.",
        "{name} — posledná správa odo mňa.\n\n{specific}.\n\nAk nie, žiadny problém.",
    ],
    "cs": [
        "Poslední pokus, {name}.\n\n{specific}.\n\nPokud to není relevantní, žádný problém.",
        "{name} — poslední zpráva ode mě.\n\n{specific}.\n\nPokud ne, žádný problém.",
    ],
    "de": [
        "Letzter Versuch, {name}.\n\n{specific}.\n\nKein Problem, falls es nicht relevant ist.",
        "{name} — letzte Nachricht von mir.\n\n{specific}.\n\nKein Problem, falls nicht.",
    ],
}

TOUCH5 = {
    "en": "I'll stop here, {name}. If you ever want to revisit, just reply to this thread.",
    "sk": "Tu sa zastavím, {name}. Ak sa niekedy budete chcieť vrátiť, stačí odpovedať na tento email.",
    "cs": "Tady se zastavím, {name}. Pokud se budete chtít někdy vrátit, stačí odpovědět na tento email.",
    "de": "Hier höre ich auf, {name}. Falls Sie irgendwann zurückkommen möchten, antworten Sie einfach auf diese E-Mail.",
}

TOUCH_MONTHLY = {
    "en": [
        "{name} — still here if the timing ever works out.",
        "Just staying visible, {name}. No pressure.",
        "{name} — happy to pick this up whenever it makes sense.",
    ],
    "sk": [
        "{name} — stále tu som, ak by čas niekedy vyšiel.",
        "Len zostávam viditeľný, {name}. Bez tlaku.",
        "{name} — rád to kedykoľvek obnovím, keď to bude dávať zmysel.",
    ],
    "cs": [
        "{name} — stále jsem tu, pokud by čas někdy vyšel.",
        "Jen zůstávám viditelný, {name}. Bez tlaku.",
        "{name} — rád to kdykoli obnovím, až to bude dávat smysl.",
    ],
    "de": [
        "{name} — ich bin noch da, falls der Zeitpunkt irgendwann passt.",
        "Ich bleibe einfach sichtbar, {name}. Kein Druck.",
        "{name} — gerne nehme ich das wieder auf, wenn es sinnvoll ist.",
    ],
}

# Touch cadence (days from touch 1)
TOUCH_CADENCE = {
    2: "touch2",   # day 2
    4: "touch3",   # day 4
    7: "touch4",   # day 7
    12: "touch5",  # day 12
    # 30, 60, 90... → monthly
}


def _sender_first_name(account: dict | None) -> str:
    name = (account or {}).get("name", "")
    if not name:
        return "Yegor"
    return name.strip().split()[0]


def _build_direct_email(language: str, greeting: str, niche: str, seed: int,
                        outreach_angle: str = "", account: dict | None = None) -> dict:
    """
    Touch 1 email. Plain, open, low-pressure.
    Body stored WITHOUT sign-off — appended at send time.
    """
    lang = language if language in TOUCH1_CTA else "en"
    _NICHE_MAP = {"dental_medical": "dental_medical", "beauty_salon": "_default",
                  "physiotherapy_wellness": "_default", "hospitality_restaurants": "_default",
                  "local_retail_ecommerce": "_default"}
    niche_key = _NICHE_MAP.get(niche, niche) if niche in _NICHE_MAP else niche
    niche_key = niche_key if niche_key in TOUCH1_SPECIFIC else "_default"

    subjects = DIRECT_SUBJECTS.get(niche_key, DIRECT_SUBJECTS["_default"]).get(lang, DIRECT_SUBJECTS["_default"]["en"])
    subject = _pick(subjects, seed, "subject")

    # Specific line: prioritize niche offer over scraped outreach_angle for the neighborly vibe
    niche_pack = TOUCH1_SPECIFIC[niche_key]
    niche_offer = _pick(niche_pack.get(lang, niche_pack["en"]), seed, "niche_value")

    # If outreach_angle exists, we can still use it, but ONLY if we aren't defaulting to the pure neighborly offer.
    # For now, as per user request to "push our offer", we ignore outreach_angle if a niche offer is available.
    if lang == "en" and niche_offer:
        specific_line = niche_offer.strip().rstrip(".") + "."
    elif outreach_angle and outreach_angle.strip() and lang == "en":
        specific_line = outreach_angle.strip().rstrip(".") + "."
    else:
        specific_line = niche_offer

    intro = _pick(TOUCH1_INTRO.get(lang, TOUCH1_INTRO["en"]), seed, "intro")
    cta = _pick(TOUCH1_CTA[lang], seed, "cta")

    # greeting → intro → specific → cta
    # No sign-off — appended at send time
    body_parts = [greeting, intro, specific_line, cta]

    # Occasional neighborly P.S. (10% chance)
    if lang == "en" and "ps_options" in LANG_PACKS[lang]:
        # Use seed to stay deterministic for this lead/touch
        ps_roll = (seed * 7 + 13) % 100
        if ps_roll < 10:  # 10% probability
            ps_line = _pick(LANG_PACKS[lang]["ps_options"], seed, "ps")
            body_parts.append(f"P.S. {ps_line}")

    body = "\n\n".join(body_parts)
    return {
        "subject": subject[:80],
        "body": body,
        "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16],
    }


def generate_followup(lead: dict, touch: int, account: dict | None = None) -> dict:
    """
    Generate a follow-up email for touch 2-5 and monthly.
    touch: 2, 3, 4, 5, or 6+ (monthly)
    Body stored WITHOUT sign-off.
    """
    language = _infer_language(lead, _normalize_language(lead.get("language", "")))
    lang = language if language in TOUCH2 else "en"

    contact_name = (lead.get("contact_name") or "").strip()
    if not contact_name:
        email_addr = (lead.get("site_emails") or lead.get("email_maps") or "").split(",")[0].strip()
        contact_name = extract_name(lead.get("brand_summary", ""), email_addr)
    name = contact_name or ""

    seed = _stable_index(lead.get("id"), lead.get("name"), touch, lang)
    
    # UK Trades follow-up path
    if language == "en" and _is_uk_trade(lead) and touch <= 5:
        touch_key = f"touch{touch}"
        if touch_key in UK_TRADES_FOLLOWUP:
            sender_name = (account or {}).get("name") or "Yegor"
            body = UK_TRADES_FOLLOWUP[touch_key].format(name=name, sender_name=sender_name)
            return {
                "subject": "Re: " + (lead.get("last_subject") or "follow-up"),
                "body": body,
                "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16],
                "touch": touch,
                "is_uk_trade": True,
            }

    niche = (lead.get("target_niche") or "").strip()
    _NICHE_MAP = {"dental_medical": "dental_medical", "beauty_salon": "_default",
                  "physiotherapy_wellness": "_default", "hospitality_restaurants": "_default",
                  "local_retail_ecommerce": "_default"}
    niche_key = _NICHE_MAP.get(niche, niche) if niche in _NICHE_MAP else niche
    niche_key = niche_key if niche_key in TOUCH1_SPECIFIC else "_default"

    outreach_angle = (lead.get("outreach_angle") or "").strip()
    niche_pack = TOUCH1_SPECIFIC[niche_key]
    specific = outreach_angle if (outreach_angle and lang == "en") else _pick(niche_pack.get(lang, niche_pack["en"]), seed, "specific")

    if touch == 2:
        template = _pick(TOUCH2[lang], seed, "t2")
        body = template.format(name=name) if name else template.format(name="").replace(", ", "").replace(" —", "").strip()
        subject = "Re: " + (lead.get("last_subject") or "follow-up")
    elif touch == 3:
        template = _pick(TOUCH3[lang], seed, "t3")
        body = template.format(name=name) if name else template.format(name="").strip()
        subject = "Re: " + (lead.get("last_subject") or "follow-up")
    elif touch == 4:
        template = _pick(TOUCH4[lang], seed, "t4")
        body = template.format(name=name, specific=specific.rstrip(".")) if name else template.format(name="", specific=specific.rstrip(".")).strip()
        subject = "Re: " + (lead.get("last_subject") or "follow-up")
    elif touch == 5:
        template = TOUCH5[lang]
        body = template.format(name=name) if name else template.format(name="").strip()
        subject = "Re: " + (lead.get("last_subject") or "follow-up")
    else:
        # Monthly
        template = _pick(TOUCH_MONTHLY[lang], seed, "monthly")
        body = template.format(name=name) if name else template.format(name="").strip()
        subject = "Re: " + (lead.get("last_subject") or "follow-up")

    return {
        "subject": subject[:80],
        "body": body.strip(),
        "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16],
        "touch": touch,
    }


def _is_uk_trade(lead: dict) -> bool:
    """Detect if lead is a UK tradesperson (plumber, electrician, HVAC, etc.)"""
    website = (lead.get("website") or lead.get("site_url") or "").lower()
    address = (lead.get("address") or "").lower()
    category = (lead.get("category") or "").lower()
    niche = (lead.get("target_niche") or "").lower()
    brand = (lead.get("brand_summary") or "").lower()
    
    # UK location markers
    uk_markers = [".co.uk", ".uk", "london", "manchester", "birmingham", "leeds", 
                  "glasgow", "liverpool", "bristol", "sheffield", "edinburgh",
                  "uk", "united kingdom", "england", "scotland", "wales"]
    is_uk = any(m in website or m in address for m in uk_markers)
    
    # Trade markers
    trade_markers = ["plumb", "electri", "hvac", "heating", "boiler", "gas", "roofer",
                     "builder", "carpenter", "joiner", "tiler", "plasterer", 
                     "handyman", "kitchen", "bathroom", "renovation",
                     "drain", "painter", "decorator", "bricklayer"]
    is_trade = any(t in category or t in niche or t in brand for t in trade_markers)
    
    return is_uk and is_trade


def _detect_platform(lead: dict) -> str:
    """Detect lead platform from website or brand info for personalization."""
    text = (lead.get("website") or "") + " " + (lead.get("brand_summary") or "")
    text_lower = text.lower()
    
    platform_map = {
        "checkatrade": "Checkatrade",
        "mybuilder": "MyBuilder", 
        "bark": "Bark",
        "trustatrader": "TrustATrader",
        "rated": "Rated People",
    }
    
    for key, platform in platform_map.items():
        if key in text_lower:
            return platform
    
    # Default fallback
    return "Checkatrade or MyBuilder"


def _generate_uk_trades_email(lead: dict, seed: int, contact_name: str) -> dict:
    """Generate UK trades A/B test email (50/50 split)."""
    # A/B selector: use lead ID for deterministic but even split
    variant = "variant_a" if (lead.get("id") or seed) % 2 == 0 else "variant_b"
    config = UK_TRADES_AB[variant]
    
    # Personalization
    name = contact_name or "there"
    platform = _detect_platform(lead)
    
    # Build body
    body = config["body_template"].format(name=name, platform=platform)
    
    return {
        "subject": config["subject"],
        "body": body,
        "fingerprint": hashlib.sha1(body.encode("utf-8")).hexdigest()[:16],
        "variant": variant,
        "word_count": config["word_count"],
    }


def generate_email(lead: dict, account: dict | None = None) -> dict:
    """Return {'subject': str, 'body': str, 'fingerprint': str} for email outreach.

    Args:
        lead: lead dict from businesses table
        account: sender account dict with 'name' and 'address' keys.
                 If None, sign-off defaults to first sender name.
    """
    # Name enrichment: prefer contact_name column (first word only), fall back to email-prefix extraction
    contact_name = (lead.get("contact_name") or "").strip()
    if contact_name:
        contact_name = contact_name.split()[0]  # first name only
    if not contact_name:
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
    
    # UK Trades A/B Test path
    if language == "en" and _is_uk_trade(lead):
        return _generate_uk_trades_email(lead, seed, contact_name)

    # Use "Hey {name}," for EN if name known (friendlier), else standard greeting
    if contact_name and language == "en":
        greeting = f"Hey {contact_name},"
    else:
        greeting = _greeting(language, contact_name, seed)

    niche = (lead.get("target_niche") or "").strip()
    outreach_angle = (lead.get("outreach_angle") or "").strip()

    return _build_direct_email(language, greeting, niche, seed,
                               outreach_angle=outreach_angle, account=account)


def generate_dm(lead: dict, channel: str = "instagram", account: dict | None = None) -> str:
    email = generate_email(lead, account=account)
    lines = [line.strip() for line in email["body"].splitlines() if line.strip()]
    dm_lines = lines[1:3] if len(lines) >= 3 else lines
    return " ".join(dm_lines)[:350]
