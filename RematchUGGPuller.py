import pandas as pd
import re
import uuid
from playwright.sync_api import sync_playwright

# ---------- Players you already use ----------
players = [
    ("Boydardi", "76561198054329313"),
    ("Othos", "76561198239409890"),
    ("SquallOwl", "76561198072504992"),
]

# ---------- Rank detection (works for Bronze → Elite) ----------
RANK_WORDS = ["Bronze", "Silver", "Gold", "Platinum", "Diamond", "Master", "Elite"]
RANK_REGEX = r"\b(" + "|".join(RANK_WORDS) + r")\b"
rank_rx = re.compile(RANK_REGEX, re.I)

def get_rank_from_card_sync(card):
    """Read accessible labels on the icon inside a match card to infer rank."""
    texts = []
    for sel in ['img[alt]', '[aria-label]', '[title]', 'svg[aria-label]', 'svg[title]']:
        nodes = card.locator(sel)
        n = nodes.count()
        for i in range(n):
            node = nodes.nth(i)
            for attr in ("alt", "aria-label", "title"):
                try:
                    val = node.get_attribute(attr)
                except Exception:
                    val = None
                if val:
                    texts.append(val)
    # also include visible text as a fallback ("Elite" sometimes printed)
    try:
        t = card.inner_text()
        if t:
            texts.append(t)
    except Exception:
        pass

    for t in texts:
        m = rank_rx.search(t)
        if m:
            return m.group(1).title()
    return None

# ---------- Match-card text parser ----------
def extract_match_summary(player_name, text, rank_override=None):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    def to_int(s):
        return int(s.replace(",", ""))

    def parse_prev_int(global_idx):
        if global_idx <= 0:
            return 0
        m = re.search(r"\d{1,3}(?:,\d{3})*", lines[global_idx - 1])
        return to_int(m.group(0)) if m else 0

    wl_rx = re.compile(r"(\d{1,3}(?:,\d{3})*)W\s+(\d{1,3}(?:,\d{3})*)L\b")
    wl_or_result_rx = re.compile(r"^(\d{1,3}(?:,\d{3})*W\s+\d{1,3}(?:,\d{3})*L|Win|Loss)\b", re.I)
    playlist_head_rx = re.compile(r"^(Ranked|Quickmatch|5v5|4v4|3v3)\b", re.I)
    visible_rank_rx = rank_rx

    # split into blocks so stats don't bleed across cards
    starts = [i for i, ln in enumerate(lines) if wl_or_result_rx.match(ln)]
    rows = []

    for si, start in enumerate(starts):
        end = starts[si + 1] if si + 1 < len(starts) else len(lines)
        block = lines[start:end]

        wins = losses = 0
        mvp = 0
        playlist = "Unknown"
        total_matches = None
        division = None
        rank = None
        goals = passes = assists = tackles = saves = 0

        # header (either "nW mL" or "Win"/"Loss")
        m = wl_rx.search(block[0])
        if m:
            wins = int(m.group(1).replace(",", ""))
            losses = int(m.group(2).replace(",", ""))
        else:
            if block[0].lower().startswith("win"):
                wins, losses = 1, 0
            elif block[0].lower().startswith("loss"):
                wins, losses = 0, 1

        # MVPs on header
        mvph = re.search(r"(\d{1,3})\s*MVPs?\b", block[0], re.I)
        if mvph:
            mvp = int(mvph.group(1))

        # scan remainder of the block
        for ln in block[1:]:
            pl = playlist_head_rx.match(ln)
            if pl:
                playlist = pl.group(1)
                mm = re.search(r"\((\d{1,3}(?:,\d{3})*)\s*matches?\)", ln, re.I)
                if mm:
                    total_matches = int(mm.group(1).replace(",", ""))

            mv = re.search(r"(\d{1,3})\s*MVPs?\b", ln, re.I)
            if mv:
                mvp = int(mv.group(1))

            if "Division" in ln:
                division = ln  # keep raw text (e.g., "Division II")

            rk = visible_rank_rx.search(ln)
            if rk and rank is None:
                rank = rk.group(1).title()

        # stat tiles (value is previous line within same block)
        for j, ln in enumerate(block):
            if ln == "Goals":
                goals = parse_prev_int(start + j)
            elif ln == "Passes":
                passes = parse_prev_int(start + j)
            elif ln == "Assists":
                assists = parse_prev_int(start + j)
            elif ln == "Tackles":
                tackles = parse_prev_int(start + j)
            elif ln == "Saves":
                saves = parse_prev_int(start + j)

        if total_matches is None:
            total_matches = 1 if (wins, losses) in [(1, 0), (0, 1)] and not m else wins + losses

        rows.append({
            "MatchGroupUID": str(uuid.uuid4()),
            "Player Name": player_name,
            "Playlist": playlist,
            "Wins": wins,
            "Losses": losses,
            "Total Matches": total_matches,
            "MVPs": mvp,
            "Division": division,
            "Rank": rank_override or rank,   # <-- rank added
            "Goals": goals,
            "Passes": passes,
            "Assists": assists,
            "Tackles": tackles,
            "Saves": saves,
        })

    return rows

# ---------- Profile stats (unchanged other than your recent additions) ----------
def extract_profile_stats(player_name, text, profile_url):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    stats = {
        "Player Name": player_name,
        "Profile URL": profile_url,

        # NEW
        "Current Rank": None,

        # “Last N Games”
        "Last # of Games": None,
        "Last # Games Win Rate": None,
        "Last # of Games Goals": None,
        "Last # of Games Saves": None,

        # Playlist WRs
        "Ranked Win Rate": None,
        "5v5 Win Rate": None,
        "4v4 Win Rate": None,
        "3v3 Win Rate": None,

        # Lifetime tiles
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

    def parse_int(s):
        m = re.search(r"-?\d{1,3}(?:,\d{3})*", s)
        return int(m.group(0).replace(",", "")) if m else None

    def parse_pct(s):
        m = re.search(r"(\d{1,3}(?:\.\d+)?)\s*%", s)
        return f"{m.group(1)}%" if m else None

    def prev_value_int(idx):
        return parse_int(lines[idx-1]) if idx > 0 else None

    def scan_near(idx, pattern, forward=8, backward=6):
        rx = re.compile(pattern, re.I)
        for j in range(max(0, idx-backward), idx):
            m = rx.search(lines[j])
            if m: return m, j
        for j in range(idx+1, min(len(lines), idx+1+forward)):
            m = rx.search(lines[j])
            if m: return m, j
        return None, None

    # --- Current Rank from "Competitive Rating" panel ---
    # Find the line with "Competitive Rating", then look nearby for a rank word.
    for i, ln in enumerate(lines):
        if "competitive rating" in ln.lower():
            # Search downward a bit; some layouts put the rank on the next line
            for j in range(i, min(i + 10, len(lines))):
                m = _rank_rx.search(lines[j])
                if m:
                    # normalize (title-case)
                    stats["Current Rank"] = m.group(1).title()
                    break
            break
    # Fallback: if the header wasn't found, try the whole text once (safe, but still word-boundary)
    if stats["Current Rank"] is None:
        m = _rank_rx.search("\n".join(lines))
        if m:
            stats["Current Rank"] = m.group(1).title()

    # --- “Last N Games” block ---
    for i, ln in enumerate(lines):
        m = re.search(r"Last\s+(\d+)\s+Games", ln, re.I)
        if not m: continue
        try: stats["Last # of Games"] = int(m.group(1))
        except ValueError: pass
        wr_m, _ = scan_near(i, r"\b\d{1,3}(?:\.\d+)?\s*%\s*WR\b")
        if wr_m and stats["Last # Games Win Rate"] is None:
            stats["Last # Games Win Rate"] = parse_pct(wr_m.group(0))
        for j in range(max(0, i-6), min(len(lines), i+12)):
            if lines[j].lower() == "goals" and stats["Last # of Games Goals"] is None:
                v = prev_value_int(j)
                if v is not None: stats["Last # of Games Goals"] = v
            if lines[j].lower() == "saves" and stats["Last # of Games Saves"] is None:
                v = prev_value_int(j)
                if v is not None: stats["Last # of Games Saves"] = v
        break

    # --- Overall W/L/M from "(123W 45L)" ---
    for ln in lines:
        m = re.search(r"\((\d{1,3}(?:,\d{3})*)W\s+(\d{1,3}(?:,\d{3})*)L\)", ln)
        if m:
            wins = int(m.group(1).replace(",", ""))
            losses = int(m.group(2).replace(",", ""))
            stats["Total Wins"] = wins
            stats["Total Losses"] = losses
            stats["Total Matches"] = wins + losses
            break

    # --- Lifetime tiles ---
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
                pct = parse_pct(lines[idx-1]) if idx > 0 else None
                if pct is None: pct = parse_pct(ln)
                stats[key] = pct
            else:
                v = prev_value_int(idx)
                if v is not None: stats[key] = v

    # --- Playlist WRs ---
    playlist_key = {"ranked": "Ranked Win Rate", "5v5": "5v5 Win Rate", "4v4": "4v4 Win Rate", "3v3": "3v3 Win Rate"}
    pl_head = re.compile(r"^(Ranked|5v5|4v4|3v3)\b", re.I)
    for i, ln in enumerate(lines):
        m = pl_head.match(ln)
        if not m: continue
        key = playlist_key[m.group(1).lower()]
        wr_m, _ = scan_near(i, r"\b\d{1,3}(?:\.\d+)?\s*%\s*WR\b", forward=6, backward=0)
        if wr_m and stats[key] is None:
            stats[key] = parse_pct(wr_m.group(0))
        if stats[key] is None:
            pct_m, _ = scan_near(i, r"\b\d{1,3}(?:\.\d+)?\s*%\b", forward=6, backward=0)
            if pct_m: stats[key] = parse_pct(pct_m.group(0))

    return stats

# ---------- Scraper (single run, per your original flow) ----------
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

            # 1) Grab the Match History container (for fallback + profile stats text)
            try:
                section = page.locator("text=Match History").first
                container = section.locator("xpath=../../../..")
                container_text = container.inner_text()
            except Exception as e:
                print(f"❌ Failed to locate Match History for {name}: {e}")
                container_text = ""

            # 2) Try to iterate each match card to pick up Rank reliably
            #    We try a few candidate selectors, then fall back to container parsing.
            cards = None
            for sel in ["[data-testid='match-card']", ".match-card", "section:has-text('matches') >> div", "article"]:
                nodes = container.locator(sel) if container_text else page.locator(sel)
                if nodes.count() > 0:
                    cards = nodes
                    break

            if cards and cards.count() > 0:
                for i in range(cards.count()):
                    card = cards.nth(i)
                    try:
                        card_text = card.inner_text()
                    except Exception:
                        card_text = ""
                    rank = get_rank_from_card_sync(card)
                    match_rows.extend(extract_match_summary(name, card_text, rank_override=rank))
            else:
                # Fallback: parse whole container text (no per-card rank; “Elite” may still be present)
                match_rows.extend(extract_match_summary(name, container_text))

            # Profile-level stats (use the same container text)
            profile_rows.append(extract_profile_stats(name, container_text, url))

        browser.close()

    return match_rows, profile_rows

# ---------- Run & export ----------
matches, profiles = scrape_rematch_profiles()

pd.DataFrame(matches).to_csv("match_history.csv", index=False)
pd.DataFrame(profiles).to_csv("profile_stats.csv", index=False)

print("\n✅ Saved match_history.csv and profile_stats.csv")
