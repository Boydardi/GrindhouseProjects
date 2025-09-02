import pandas as pd
import re
import uuid
from playwright.sync_api import sync_playwright

players = [
    ("Boydardi", "76561198054329313"),
    ("Othos", "76561198239409890"),
    ("SquallOwl", "76561198072504992")
]

def extract_match_summary(player_name, text):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # ---- helpers ----
    def to_int(s):
        return int(s.replace(",", ""))

    def parse_prev_int(idx):
        if idx <= 0: 
            return 0
        m = re.search(r"\d{1,3}(?:,\d{3})*", lines[idx-1])
        return to_int(m.group(0)) if m else 0

    # A block starts at either "<n>W <m>L" OR "Win"/"Loss"
    wl_rx = re.compile(r"(\d{1,3}(?:,\d{3})*)W\s+(\d{1,3}(?:,\d{3})*)L\b")
    wl_or_result_rx = re.compile(r"^(\d{1,3}(?:,\d{3})*W\s+\d{1,3}(?:,\d{3})*L|Win|Loss)\b", re.I)
    playlist_head_rx = re.compile(r"^(Ranked|Quickmatch|5v5|4v4|3v3)\b", re.I)

    # Playwright helper: try to read the rank from the card's icon
RANK_WORDS = ["Bronze","Silver","Gold","Platinum","Diamond","Master","Elite"]

rank_rx = r"\b(" + "|".join(RANK_WORDS) + r")\b"

async def get_rank_from_card(card):
    # Collect accessible strings around images/svgs/spans
    texts = []

    # Common attributes that may carry the label
    for sel in [
        'img[alt]',
        '[aria-label]',
        '[title]',
        'svg[aria-label] use[aria-label]',  # sometimes nested
    ]:
        nodes = card.locator(sel)
        n = await nodes.count()
        for i in range(n):
            handle = await nodes.nth(i).element_handle()
            if not handle:
                continue
            for attr in ("alt", "aria-label", "title"):
                try:
                    val = await handle.get_attribute(attr)
                except Exception:
                    val = None
                if val:
                    texts.append(val)

    # Also grab any plain text near the icon row
    try:
        texts.append((await card.text_content()) or "")
    except Exception:
        pass

    import re
    rx = re.compile(rank_rx, re.I)
    for t in texts:
        m = rx.search(t)
        if m:
            return m.group(1).title()  # normalized
    return None


    # ---- find block starts ----
    starts = [i for i, ln in enumerate(lines) if wl_or_result_rx.match(ln)]
    rows = []

    for si, start in enumerate(starts):
        end = starts[si + 1] if si + 1 < len(starts) else len(lines)
        block = lines[start:end]

        # Defaults
        wins = losses = 0
        mvp = 0
        playlist = "Unknown"
        total_matches = None
        division = None
        goals = passes = assists = tackles = saves = 0

        # Header: either "<n>W <m>L" or "Win"/"Loss"
        m = wl_rx.search(block[0])
        if m:
            wins = to_int(m.group(1))
            losses = to_int(m.group(2))
        else:
            # Single-match card
            if block[0].lower().startswith("win"):
                wins, losses = 1, 0
            elif block[0].lower().startswith("loss"):
                wins, losses = 0, 1

        # MVPs can be on header line (e.g., "… 2 MVPs") or inside block
        mvph = re.search(r"(\d{1,3})\s*MVPs?\b", block[0], re.I)
        if mvph:
            mvp = int(mvph.group(1))

        # Scan the rest of the block
        for ln in block[1:]:
            pl = playlist_head_rx.match(ln)
            if pl:
                playlist = pl.group(1)
                # "3v3 (14 matches)"
                mm = re.search(r"\((\d{1,3}(?:,\d{3})*)\s*matches?\)", ln, re.I)
                if mm:
                    total_matches = to_int(mm.group(1))

            mv = re.search(r"(\d{1,3})\s*MVPs?\b", ln, re.I)
            if mv:
                mvp = int(mv.group(1))

            if "Division" in ln:
                division = ln  # keep raw text (e.g., "Division II")

        # Stat tiles (value on previous line within the same block)
        # loop over indices in the block so we can look back safely
        for j, ln in enumerate(block):
            if ln == "Goals":
                goals = parse_prev_int(start + j)  # use global index for prev lookup
            elif ln == "Passes":
                passes = parse_prev_int(start + j)
            elif ln == "Assists":
                assists = parse_prev_int(start + j)
            elif ln == "Tackles":
                tackles = parse_prev_int(start + j)
            elif ln == "Saves":
                saves = parse_prev_int(start + j)

        # If "(## matches)" wasn’t present, fall back:
        if total_matches is None:
            # For single-match "Win/Loss" cards, force 1.
            if (wins, losses) in [(1, 0), (0, 1)] and not m:
                total_matches = 1
            else:
                total_matches = wins + losses

        rows.append({
            "MatchGroupUID": str(uuid.uuid4()),
            "Player Name": player_name,
            "Playlist": playlist,
            "Wins": wins,
            "Losses": losses,
            "Total Matches": total_matches,
            "MVPs": mvp,
            "Division": division,
            "Goals": goals,
            "Passes": passes,
            "Assists": assists,
            "Tackles": tackles,
            "Saves": saves,
        })

    return rows

def extract_profile_stats(player_name, text, profile_url):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    stats = {
        "Player Name": player_name,
        "Profile URL": profile_url,

        # “Last N Games” block
        "Last # of Games": None,
        "Last # Games Win Rate": None,
        "Last # of Games Goals": None,
        "Last # of Games Saves": None,

        # Playlist win rates
        "Ranked Win Rate": None,
        "5v5 Win Rate": None,
        "4v4 Win Rate": None,
        "3v3 Win Rate": None,

        # Overall tallies
        "Total Wins": None,
        "Total Losses": None,
        "Total Matches": None,
        "Total Goals": None,
        "Total Assists": None,
        "Total MVPs": None,
        "Total Passes": None,
        "Total Tackles": None,
        "Total Steals": None,
        "Total Saves": None,
        "Total Shots": None,
        "Total Shot %": None,
    }

    # ---------- helpers ----------
    def parse_int(s):
        m = re.search(r"-?\d{1,3}(?:,\d{3})*", s)
        if not m: 
            return None
        try:
            return int(m.group(0).replace(",", ""))
        except ValueError:
            return None

    def parse_pct(s):
        m = re.search(r"(\d{1,3}(?:\.\d+)?)\s*%", s)
        return f"{m.group(1)}%" if m else None

    def prev_value_int(idx):
        return parse_int(lines[idx-1]) if idx > 0 else None

    def scan_near(idx, pattern, forward=8, backward=6):
        rx = re.compile(pattern, re.I)
        # backward first (useful for Last N Games WR that precedes the label)
        for j in range(max(0, idx-backward), idx):
            m = rx.search(lines[j])
            if m:
                return m, j
        # then forward
        for j in range(idx+1, min(len(lines), idx+1+forward)):
            m = rx.search(lines[j])
            if m:
                return m, j
        return None, None

    # ---------- Last N Games ----------
    for i, ln in enumerate(lines):
        m = re.search(r"Last\s+(\d+)\s+Games", ln, re.I)
        if not m:
            continue
        # N
        try:
            stats["Last # of Games"] = int(m.group(1))
        except ValueError:
            pass

        # WR near the label (often appears BEFORE)
        wr_m, _ = scan_near(i, r"\b\d{1,3}(?:\.\d+)?\s*%\s*WR\b")
        if wr_m and stats["Last # Games Win Rate"] is None:
            stats["Last # Games Win Rate"] = parse_pct(wr_m.group(0))

        # Nearby Goals / Saves (value usually on the previous line)
        for j in range(max(0, i-6), min(len(lines), i+12)):
            if lines[j].lower() == "goals" and stats["Last # of Games Goals"] is None:
                v = prev_value_int(j)
                if v is not None:
                    stats["Last # of Games Goals"] = v
            if lines[j].lower() == "saves" and stats["Last # of Games Saves"] is None:
                v = prev_value_int(j)
                if v is not None:
                    stats["Last # of Games Saves"] = v
        break  # only the first Last N block

    # ---------- Overall W/L/M from header like “(121W 87L)” ----------
    for ln in lines:
        m = re.search(r"\((\d{1,3}(?:,\d{3})*)W\s+(\d{1,3}(?:,\d{3})*)L\)", ln)
        if m:
            wins = int(m.group(1).replace(",", ""))
            losses = int(m.group(2).replace(",", ""))
            stats["Total Wins"] = wins
            stats["Total Losses"] = losses
            stats["Total Matches"] = wins + losses
            break

    # ---------- Lifetime stat tiles (value on the previous line) ----------
    label_to_key = {
        "Goals": "Total Goals",
        "Assists": "Total Assists",
        "MVPs": "Total MVPs",
        "Passes": "Total Passes",
        "Tackles": "Total Tackles",
        "Steals": "Total Steals",
        "Saves": "Total Saves",
        "Shots": "Total Shots",
        "Shot %": "Total Shot %",
        "Shot%": "Total Shot %",
    }
    for idx, ln in enumerate(lines):
        if ln in label_to_key:
            key = label_to_key[ln]
            if key == "Total Shot %":
                # try prev or same line
                pct = parse_pct(lines[idx-1]) if idx > 0 else None
                if pct is None:
                    pct = parse_pct(ln)
                stats[key] = pct
            else:
                v = prev_value_int(idx)
                if v is not None:
                    stats[key] = v

    # ---------- Playlist Win Rates (Ranked/5v5/4v4/3v3) ----------
    playlist_key = {
        "ranked": "Ranked Win Rate",
        "5v5": "5v5 Win Rate",
        "4v4": "4v4 Win Rate",
        "3v3": "3v3 Win Rate",
    }
    pl_head = re.compile(r"^(Ranked|5v5|4v4|3v3)\b", re.I)
    for i, ln in enumerate(lines):
        m = pl_head.match(ln)
        if not m:
            continue
        key = playlist_key[m.group(1).lower()]
        # Prefer “##% WR ...” near the header; fallback to any percent nearby
        wr_m, _ = scan_near(i, r"\b\d{1,3}(?:\.\d+)?\s*%\s*WR\b", forward=6, backward=0)
        if wr_m and stats[key] is None:
            stats[key] = parse_pct(wr_m.group(0))
        if stats[key] is None:
            pct_m, _ = scan_near(i, r"\b\d{1,3}(?:\.\d+)?\s*%\b", forward=6, backward=0)
            if pct_m:
                stats[key] = parse_pct(pct_m.group(0))

    return stats

def scrape_rematch_profiles():
    match_rows = []
    profile_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for name, platform_id in players:
            url = f"https://u.gg/rematch/profile/steam/{name}/{platform_id}"
            print(f"🔍 Loading {url}")
            page.goto(url)
            page.wait_for_timeout(5000)

            try:
                section = page.locator("text=Match History").first
                container = section.locator("xpath=../../../..")
                visible_text = container.inner_text()
            except Exception as e:
                print(f"❌ Failed to extract for {name}: {e}")
                visible_text = ""

            match_rows.extend(extract_match_summary(name, visible_text))
            profile_rows.append(extract_profile_stats(name, visible_text, url))

        browser.close()

    return match_rows, profile_rows

# Run and export
matches, profiles = scrape_rematch_profiles()

pd.DataFrame(matches).to_csv("match_history.csv", index=False)
pd.DataFrame(profiles).to_csv("profile_stats.csv", index=False)

print("\n✅ Saved match_history.csv and profile_stats.csv")
