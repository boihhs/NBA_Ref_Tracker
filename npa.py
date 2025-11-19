import requests
import pandas as pd

SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"


def fetch_espn_summary(event_id: int, timeout: float = 10.0) -> dict:
    params = {"event": event_id}
    r = requests.get(SUMMARY_URL, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def parse_game_time(period, clock_str):
    """
    Convert (period, MM:SS clock) -> elapsed seconds from start of game.
    Assumes:
      - Regulation: 4 x 12:00
      - OT: 5:00 each.
    """
    if period is None or clock_str is None:
        return None

    try:
        mins, secs = clock_str.split(":")
        remaining = int(mins) * 60 + int(secs)
    except Exception:
        return None

    if period <= 4:
        period_len = 12 * 60
        prev_total = (period - 1) * 12 * 60
    else:
        period_len = 5 * 60
        prev_total = 4 * 12 * 60 + (period - 5) * 5 * 60

    elapsed_in_period = period_len - remaining
    return prev_total + elapsed_in_period


def get_home_away_meta(data: dict):
    """
    Extract home/away (id, abbr, name, short).
    """
    header = data.get("header", {})
    comps = header.get("competitions") or []
    if not comps:
        return None, None

    comp = comps[0]
    competitors = comp.get("competitors") or []

    home = away = None
    for c in competitors:
        team = c.get("team") or {}
        meta = {
            "id": team.get("id"),
            "abbr": team.get("abbreviation"),
            "name": (
                team.get("displayName")
                or team.get("shortDisplayName")
                or team.get("name")
            ),
            "short": team.get("shortDisplayName") or team.get("abbreviation"),
        }
        if c.get("homeAway") == "home":
            home = meta
        elif c.get("homeAway") == "away":
            away = meta

    return home, away


def build_wp_df_from_summary(event_id: int, data: dict) -> pd.DataFrame:
    """
    Build WP timeline joined to plays.

    Columns include:
      - play_id_int (for order)
      - period, clock, home_score, away_score
      - home_win_prob, away_win_prob
      - description
      - team_id/team_abbr/team_name from play.team
      - primary_athlete_id (first participant if present)
      - participants_raw (list of participant ids for later lookup)
      - elapsed (seconds from game start)
      - *_before fields via shift(1), including times.
    """
    wp = data.get("winprobability")
    if not wp:
        raise ValueError(f"No win probability data for event {event_id}")

    plays = data.get("plays", [])
    plays_by_id = {str(p.get("id")): p for p in plays}

    rows = []
    for w in wp:
        pid = str(w.get("playId"))
        play = plays_by_id.get(pid, {})

        home_wp = w.get("homeWinPercentage")
        tie_wp = w.get("tiePercentage", 0.0) or 0.0
        away_wp = (
            1.0 - (home_wp or 0.0) - tie_wp
            if home_wp is not None
            else None
        )

        period = (play.get("period") or {}).get("number")
        clock = (play.get("clock") or {}).get("displayValue")
        home_score = play.get("homeScore")
        away_score = play.get("awayScore")

        team = play.get("team") or {}
        team_abbr = team.get("abbreviation") or team.get("shortDisplayName")
        team_id = team.get("id")
        team_name = (
            team.get("displayName")
            or team.get("shortDisplayName")
            or team.get("name")
            or team_abbr
        )

        participants = play.get("participants") or []
        athlete_ids = []
        for p in participants:
            athlete = p.get("athlete") or {}
            aid = athlete.get("id")
            if aid is not None:
                athlete_ids.append(str(aid))

        rows.append(
            {
                "play_id": pid,
                "period": period,
                "clock": clock,
                "home_score": home_score,
                "away_score": away_score,
                "home_win_prob": home_wp,
                "away_win_prob": away_wp,
                "description": play.get("text"),
                "team_id": team_id,
                "team_abbr": team_abbr,
                "team_name": team_name,
                "primary_athlete_id": athlete_ids[0] if athlete_ids else None,
                "participants_raw": athlete_ids,
            }
        )

    df = pd.DataFrame(rows)

    # Order by play id so "previous" is well-defined
    df["play_id_int"] = df["play_id"].astype("int64")
    df = df.sort_values("play_id_int").reset_index(drop=True)

    # Elapsed time for each sample
    df["elapsed"] = df.apply(
        lambda r: parse_game_time(r["period"], r["clock"]),
        axis=1,
    )

    # Previous WP sample + time + period/clock
    df["home_wp_before"] = df["home_win_prob"].shift(1)
    df["away_wp_before"] = df["away_win_prob"].shift(1)
    df["elapsed_before"] = df["elapsed"].shift(1)
    df["period_before"] = df["period"].shift(1)
    df["clock_before"] = df["clock"].shift(1)

    return df


def get_game_officials_from_summary(data: dict):
    """
    Extract officials from:
      - gameInfo.officials
      - header.competitions[0].officials or .details.officials
      - else boxscore.officials
    """
    officials = []

    header = data.get("header", {})
    comps = header.get("competitions") or []
    if comps:
        comp = comps[0]
        comp_offs = (
            comp.get("officials")
            or (comp.get("details") or {}).get("officials")
            or []
        )
        for o in comp_offs:
            name = (
                o.get("displayName")
                or o.get("fullName")
                or o.get("name")
                or "Unknown"
            )
            pos = o.get("position") or o.get("role") or {}
            if isinstance(pos, dict):
                pos = pos.get("displayName") or pos.get("name") or ""
            officials.append((name, pos))

    if not officials:
        game_info = data.get("gameInfo") or {}
        for o in game_info.get("officials") or []:
            name = (
                o.get("displayName")
                or o.get("fullName")
                or o.get("name")
                or "Unknown"
            )
            pos = o.get("position") or o.get("role") or {}
            if isinstance(pos, dict):
                pos = pos.get("displayName") or pos.get("name") or ""
            officials.append((name, pos))

    if not officials:
        box = data.get("boxscore") or data.get("boxScore") or {}
        for o in box.get("officials") or []:
            name = (
                o.get("displayName")
                or o.get("fullName")
                or o.get("name")
                or "Unknown"
            )
            pos = o.get("position") or o.get("role") or {}
            if isinstance(pos, dict):
                pos = pos.get("displayName") or pos.get("name") or ""
            officials.append((name, pos))

    return officials


def build_athlete_lookup_from_summary(data: dict):
    """
    Build athlete_id -> {name, team_id, team_abbr} from boxscore.players statistics blocks.
    """
    lookup = {}
    box = data.get("boxscore") or data.get("boxScore") or {}
    for team_block in box.get("players") or []:
        team = team_block.get("team") or {}
        team_id = team.get("id")
        team_abbr = team.get("abbreviation") or team.get("shortDisplayName")

        for stat in team_block.get("statistics") or []:
            for athlete_entry in stat.get("athletes") or []:
                athlete = athlete_entry.get("athlete") or {}
                aid = athlete.get("id")
                if aid is None:
                    continue

                lookup[str(aid)] = {
                    "name": athlete.get("displayName")
                    or athlete.get("shortName")
                    or athlete.get("fullName")
                    or "Unknown",
                    "team_id": team_id,
                    "team_abbr": team_abbr,
                }

    return lookup


def infer_foul_team(row, home_meta, away_meta):
    """
    Infer which TEAM the foul is on (no player detail).

    Priority:
      1) row.team_abbr directly matches home/away
      2) row.team_id matches home/away
      3) Fallback: team name/abbr appears in description
      4) Else 'Unknown'
    """
    desc = (row.description or "").lower()

    # Direct from team_abbr if present
    if row.team_abbr:
        if home_meta and row.team_abbr == home_meta["abbr"]:
            return home_meta["abbr"]
        if away_meta and row.team_abbr == away_meta["abbr"]:
            return away_meta["abbr"]

    # Direct from team_id
    if pd.notna(row.team_id):
        team_id = str(int(row.team_id)) if isinstance(row.team_id, (int, float)) else str(row.team_id)
        if home_meta and team_id == str(home_meta.get("id")):
            return home_meta["abbr"]
        if away_meta and team_id == str(away_meta.get("id")):
            return away_meta["abbr"]

    # Fallback: text search
    if home_meta:
        h_abbr = (home_meta["abbr"] or "").lower()
        h_name = (home_meta["name"] or "").lower()
        h_short = (home_meta["short"] or "").lower()
    else:
        h_abbr = h_name = h_short = ""

    if away_meta:
        a_abbr = (away_meta["abbr"] or "").lower()
        a_name = (away_meta["name"] or "").lower()
        a_short = (away_meta["short"] or "").lower()
    else:
        a_abbr = a_name = a_short = ""

    if any(s and s in desc for s in (h_abbr, h_short, h_name)):
        return home_meta["abbr"] if home_meta else "HOME"
    if any(s and s in desc for s in (a_abbr, a_short, a_name)):
        return away_meta["abbr"] if away_meta else "AWAY"

    return "Unknown"


def infer_foul_on(row, home_meta, away_meta, athlete_lookup):
    """
    Return a display label for who the foul is on, preferring player name + team.
    """
    foul_team = infer_foul_team(row, home_meta, away_meta)

    athlete_label = None
    aid = row.primary_athlete_id
    if pd.notna(aid):
        aid_str = str(int(aid)) if isinstance(aid, (int, float)) else str(aid)
        athlete = athlete_lookup.get(aid_str)
        if athlete:
            a_name = athlete.get("name")
            a_team = athlete.get("team_abbr") or foul_team
            if a_name and a_team and foul_team == "Unknown":
                foul_team = a_team
            athlete_label = (
                f"{a_team} - {a_name}" if a_team else a_name
            )

    if athlete_label:
        return athlete_label
    return foul_team


def list_event_ids_for_season(season_start_year: int, timeout: float = 10.0):
    """
    Collect ESPN event ids for a season defined as:
      Oct 1 of season_start_year through Jun 30 of season_start_year+1.

    This hits the ESPN scoreboard endpoint once per day in that window.
    """
    import datetime as dt

    start_date = dt.date(season_start_year, 10, 1)
    end_date = dt.date(season_start_year + 1, 6, 30)

    event_ids = []
    date = start_date
    while date <= end_date:
        params = {"dates": date.strftime("%Y%m%d")}
        resp = requests.get(SCOREBOARD_URL, params=params, timeout=timeout)
        if resp.ok:
            sb = resp.json()
            for ev in sb.get("events") or []:
                ev_id = ev.get("id")
                if ev_id:
                    event_ids.append(int(ev_id))
        else:
            print(f"Scoreboard miss {date}: {resp.status_code}")
        date += dt.timedelta(days=1)

    # Deduplicate while preserving order
    seen = set()
    unique_ids = []
    for ev_id in event_ids:
        if ev_id not in seen:
            seen.add(ev_id)
            unique_ids.append(ev_id)
    return unique_ids


def extract_foul_rows(event_id: int, data: dict):
    """
    Return a list of dictionaries containing the foul rows for a single game,
    shaped for CSV export.
    """
    home_meta, away_meta = get_home_away_meta(data)
    home_abbr = (home_meta or {}).get("abbr") or ""
    athlete_lookup = build_athlete_lookup_from_summary(data)
    officials = get_game_officials_from_summary(data)
    official_names = [o[0] for o in officials][:3]
    while len(official_names) < 3:
        official_names.append("")

    # Index plays by id for quick access to subtype/descriptor
    plays_by_id = {str(p.get("id")): p for p in data.get("plays", [])}

    df = build_wp_df_from_summary(event_id, data)
    fouls = df[df["description"].str.contains("foul", case=False, na=False)].copy()

    rows = []
    for _, r in fouls.iterrows():
        period = int(r.period) if pd.notna(r.period) else None
        clock = r.clock or ""
        team_abbr = infer_foul_team(r, home_meta, away_meta)

        fouler = None
        aid = r.primary_athlete_id
        if pd.notna(aid):
            a = athlete_lookup.get(str(int(aid)) if isinstance(aid, (int, float)) else str(aid))
            if a:
                fouler = a.get("name")

        play_obj = plays_by_id.get(str(r.play_id))
        subtype = ""
        descriptor = ""
        if play_obj:
            subtype = (play_obj.get("type") or {}).get("text") or ""
            descriptor = play_obj.get("shortDescription") or ""

        rows.append(
            {
                "Period": period,
                "Clock": clock,
                "Team": team_abbr,
                "Fouler": fouler or infer_foul_on(r, home_meta, away_meta, athlete_lookup),
                "Subtype": subtype,
                "Descriptor": descriptor,
                "Official1": official_names[0],
                "Official2": official_names[1],
                "Official3": official_names[2],
                "Home_Team": home_abbr,
                "Home_Score": int(r.home_score) if pd.notna(r.home_score) else "",
                "Away_Score": int(r.away_score) if pd.notna(r.away_score) else "",
                "WinProb_Current": r.home_win_prob,
                "WinProb_Previous": r.home_wp_before,
                "Clock_Previous_WP": r.clock_before,
            }
        )

    return rows


def print_foul_winprob(event_id: int, timeout: float = 10.0) -> None:
    """
    Prints:
      - Officials.
      - For each foul (desc contains 'foul'):
          Prev time (Q, clock), Foul time (Q, clock),
          Δt in seconds,
          Home/Away WP before and at foul,
          ΔWP_home = WP_home_at - WP_home_before,
          Team the foul is on (team-level),
          Description.
    """
    data = fetch_espn_summary(event_id, timeout=timeout)

    # Officials
    officials = get_game_officials_from_summary(data)
    if officials:
        print(f"Officials for event {event_id}:")
        for name, pos in officials:
            if pos:
                print(f"- {name} ({pos})")
            else:
                print(f"- {name}")
        print()
    else:
        print(f"Officials for event {event_id}: [none found in summary]\n")

    home_meta, away_meta = get_home_away_meta(data)
    athlete_lookup = build_athlete_lookup_from_summary(data)

    # WP timeline
    df = build_wp_df_from_summary(event_id, data)

    # Foul events (any description mentioning "foul")
    fouls = df[df["description"].str.contains("foul", case=False, na=False)].copy()
    if fouls.empty:
        print("No fouls found with associated win probability entries.")
        return

    for _, r in fouls.iterrows():
        # Foul (current) time
        period = int(r.period) if pd.notna(r.period) else -1
        clock = r.clock or ""
        t_at = r.elapsed

        # Previous WP sample time
        p_before = int(r.period_before) if pd.notna(r.period_before) else None
        c_before = r.clock_before if pd.notna(r.clock_before) else None
        t_before = r.elapsed_before

        # Scores at foul (already updated for event)
        hs = int(r.home_score) if pd.notna(r.home_score) else 0
        a_s = int(r.away_score) if pd.notna(r.away_score) else 0

        # WPs
        h_before = r.home_wp_before
        h_at = r.home_win_prob
        a_before = r.away_wp_before
        a_at = r.away_win_prob

        # Δt (seconds between prev WP and foul WP)
        dt = t_at - t_before

        # ΔWP_home (impact of foul event)
        dwp_home = h_at - h_before

        foul_on = infer_foul_on(r, home_meta, away_meta, athlete_lookup)

        prev_time_str = (
            f"Q{p_before} {c_before}" if p_before is not None and c_before else "N/A"
        )
        foul_time_str = f"Q{period} {clock}"

        print(
            f"Prev: {prev_time_str} -> Foul: {foul_time_str} | "
            f"Score {hs:3d}-{a_s:3d} | "
            f"Home WP: {h_before:6.3f} -> {h_at:6.3f} | "
            f"Away WP: {a_before:6.3f} -> {a_at:6.3f} | "
            f"ΔWP_home={dwp_home:+6.3f} | Δt={dt:6.1f}s | "
            f"Foul on: {foul_on} | {r.description}"
        )


# Example:
# print_foul_winprob(401283399)


def export_fouls_for_season(event_ids, season_label: str, output_root: str = "exports", timeout: float = 10.0):
    """
    Given a list of ESPN event ids, export one CSV per game into output_root/season_label/.
    """
    import os

    season_dir = os.path.join(output_root, season_label)
    os.makedirs(season_dir, exist_ok=True)

    for ev_id in event_ids:
        try:
            data = fetch_espn_summary(ev_id, timeout=timeout)
            rows = extract_foul_rows(ev_id, data)
            if not rows:
                continue
            df = pd.DataFrame(rows)
            csv_path = os.path.join(season_dir, f"{ev_id}.csv")
            df.to_csv(csv_path, index=False)
            print(f"Wrote {csv_path} ({len(rows)} fouls)")
        except Exception as exc:
            print(f"Failed event {ev_id}: {exc}")


def export_last_three_seasons(output_root: str = "exports", timeout: float = 10.0):
    """
    Helper to export the last three NBA seasons (relative to today).

    Seasons are labeled like '2024-25' and cover Oct 1 - Jun 30.
    """
    import datetime as dt

    today = dt.date.today()
    # NBA "season start year" is the year in which the season begins (Oct)
    # If before July, current season started previous calendar year.
    if today.month >= 7:
        current_start_year = today.year
    else:
        current_start_year = today.year - 1

    seasons = [current_start_year - i for i in range(3)]

    for start_year in seasons:
        label = f"{start_year}-{(start_year + 1) % 100:02d}"
        print(f"Collecting season {label}...")
        event_ids = list_event_ids_for_season(start_year, timeout=timeout)
        print(f"Found {len(event_ids)} events for {label}")
        export_fouls_for_season(event_ids, label, output_root=output_root, timeout=timeout)


if __name__ == "__main__":
    export_last_three_seasons()
